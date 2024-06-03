/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.timeseries.rest.handler;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.model.ADTask;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;

/**
 * Get job to make sure job has been stopped before updating a config.
 */
public class ConfigUpdateConfirmer<IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType>> {

    private final Logger logger = LogManager.getLogger(ConfigUpdateConfirmer.class);

    private final TaskManagerType taskManager;
    private final TransportService transportService;

    public ConfigUpdateConfirmer(TaskManagerType taskManager, TransportService transportService) {
        this.taskManager = taskManager;
        this.transportService = transportService;
    }

    /**
     * Get job for update/delete config.
     * If job exist, will return error message; otherwise, execute function.
     *
     * @param clusterService OS cluster service
     * @param client OS node client
     * @param id job identifier
     * @param listener Listener to send response
     * @param function time series function
     * @param xContentRegistry Registry which is used for XContentParser
     */
    public void confirmJobRunning(
        ClusterService clusterService,
        Client client,
        String id,
        ActionListener listener,
        ExecutorFunction function,
        NamedXContentRegistry xContentRegistry
    ) {
        // forecasting and ad share the same job index
        if (clusterService.state().metadata().indices().containsKey(CommonName.JOB_INDEX)) {
            GetRequest request = new GetRequest(CommonName.JOB_INDEX).id(id);
            client
                .get(
                    request,
                    ActionListener.wrap(response -> onGetJobResponseForWrite(response, listener, function, xContentRegistry), exception -> {
                        logger.error("Fail to get job: " + id, exception);
                        listener.onFailure(exception);
                    })
                );
        } else {
            function.execute();
        }
    }

    private void onGetJobResponseForWrite(
        GetResponse response,
        ActionListener listener,
        ExecutorFunction function,
        NamedXContentRegistry xContentRegistry
    ) {
        if (response.isExists()) {
            String jobId = response.getId();
            if (jobId != null) {
                // check if job is running, if yes, we can't delete the config
                try (
                    XContentParser parser = RestHandlerUtils
                        .createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    Job adJob = Job.parse(parser);
                    if (adJob.isEnabled()) {
                        listener.onFailure(new OpenSearchStatusException("Job is running: " + jobId, RestStatus.BAD_REQUEST));
                        return;
                    }
                } catch (IOException e) {
                    String message = "Failed to parse job " + jobId;
                    logger.error(message, e);
                    listener.onFailure(new OpenSearchStatusException(message, RestStatus.BAD_REQUEST));
                }
            }
        }
        function.execute();
    }

    /**
     * Confirm if any historical or run once is running. If there is still any left over tasks running,
     * listener returns failure complaining task running. Otherwise, listener response returns null
     * (indicating no batch running).
     * @param configId Config id
     * @param tasks tasks to check.
     * @param listener to return response or failure.
     */
    public void confirmBatchRunning(String configId, List<TaskTypeEnum> tasks, ActionListener<Void> listener) {
        taskManager.getAndExecuteOnLatestConfigLevelTask(configId, tasks, (task) -> {
            if (task.isPresent() && !task.get().isDone()) {
                // can't update config if there is task running
                String batchTaskName = task.get() instanceof ADTask ? "Historical" : "Run once";
                listener.onFailure(new OpenSearchStatusException(batchTaskName + " is running", RestStatus.BAD_REQUEST));
            } else {
                listener.onResponse(null);
            }
        }, transportService, false, listener); // false means don't reset task state as inactive/stopped state
    }
}
