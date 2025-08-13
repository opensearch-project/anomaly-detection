/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.constant.CommonMessages.FAIL_TO_DELETE_CONFIG;
import static org.opensearch.timeseries.util.ParseUtils.resolveUserAndExecute;
import static org.opensearch.timeseries.util.ParseUtils.verifyResourceAccessAndProcessRequest;
import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.model.ADTask;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public abstract class BaseDeleteConfigTransportAction<TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType>, ConfigType extends Config>
    extends HandledTransportAction<DeleteConfigRequest, DeleteResponse> {

    private static final Logger LOG = LogManager.getLogger(BaseDeleteConfigTransportAction.class);

    private final Client client;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private NamedXContentRegistry xContentRegistry;
    private final TaskManagerType taskManager;
    private volatile Boolean filterByEnabled;
    private final NodeStateManager nodeStateManager;
    private final AnalysisType analysisType;
    private final String stateIndex;
    private final Class<ConfigType> configTypeClass;
    private final List<TaskTypeEnum> batchTaskTypes;
    protected final String configIndexName;

    public BaseDeleteConfigTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        NodeStateManager nodeStateManager,
        TaskManagerType taskManager,
        String deleteConfigAction,
        Setting<Boolean> filterByBackendRoleSetting,
        AnalysisType analysisType,
        String stateIndex,
        Class<ConfigType> configTypeClass,
        List<TaskTypeEnum> historicalTaskTypes,
        String configIndexName
    ) {
        super(deleteConfigAction, transportService, actionFilters, DeleteConfigRequest::new);
        this.transportService = transportService;
        this.client = client;
        this.clusterService = clusterService;
        this.xContentRegistry = xContentRegistry;
        this.taskManager = taskManager;
        this.nodeStateManager = nodeStateManager;
        filterByEnabled = filterByBackendRoleSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(filterByBackendRoleSetting, it -> filterByEnabled = it);

        this.analysisType = analysisType;
        this.stateIndex = stateIndex;
        this.configTypeClass = configTypeClass;
        this.batchTaskTypes = historicalTaskTypes;
        this.configIndexName = configIndexName;
    }

    @Override
    protected void doExecute(Task task, DeleteConfigRequest request, ActionListener<DeleteResponse> actionListener) {
        String configId = request.getConfigID();
        LOG.info("Delete job {}", configId);
        User user = ParseUtils.getUserContext(client);
        ActionListener<DeleteResponse> listener = wrapRestActionListener(actionListener, FAIL_TO_DELETE_CONFIG);

        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            System.out.println("Verifying resource access and processing request");
            verifyResourceAccessAndProcessRequest(
                () -> deleteConfigIfNotRunning(configId, listener),
                () -> resolveUserAndExecute(
                    user,
                    configId,
                    filterByEnabled,
                    listener,
                    (input) -> deleteConfigIfNotRunning(configId, listener),
                    client,
                    clusterService,
                    xContentRegistry,
                    configTypeClass
                )
            );

        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    private void deleteConfigIfNotRunning(String configId, ActionListener<DeleteResponse> listener) {
        nodeStateManager.getConfig(configId, analysisType, config -> {
            if (config.isEmpty()) {
                LOG.info("Can't find config {}", configId);
                taskManager.deleteTasks(configId, () -> deleteJobDoc(configId, listener), listener);
                return;
            }

            // Check if there is a realtime job or batch analysis task running
            getJob(configId, listener, () -> {
                taskManager.getAndExecuteOnLatestConfigLevelTask(configId, batchTaskTypes, configTask -> {
                    if (configTask.isPresent() && !configTask.get().isDone()) {
                        String batchTaskName = configTask.get() instanceof ADTask ? "Historical" : "Run once";
                        listener.onFailure(new OpenSearchStatusException(batchTaskName + " is running", RestStatus.BAD_REQUEST));
                    } else {
                        taskManager.deleteTasks(configId, () -> deleteJobDoc(configId, listener), listener);
                    }
                }, transportService, false, listener);
            });
        }, listener);
    }

    private void deleteJobDoc(String configId, ActionListener<DeleteResponse> listener) {
        LOG.info("Delete job {}", configId);
        DeleteRequest deleteRequest = new DeleteRequest(CommonName.JOB_INDEX, configId)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.delete(deleteRequest, ActionListener.wrap(response -> {
            if (response.getResult() == DocWriteResponse.Result.DELETED || response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                deleteStateDoc(configId, listener);
            } else {
                String message = "Fail to delete job " + configId;
                LOG.error(message);
                listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> {
            LOG.error("Failed to delete job for " + configId, exception);
            if (exception instanceof IndexNotFoundException) {
                deleteStateDoc(configId, listener);
            } else {
                LOG.error("Failed to delete job", exception);
                listener.onFailure(exception);
            }
        }));
    }

    private void deleteStateDoc(String configId, ActionListener<DeleteResponse> listener) {
        LOG.info("Delete config state {}", configId);
        DeleteRequest deleteRequest = new DeleteRequest(stateIndex, configId);
        client.delete(deleteRequest, ActionListener.wrap(response -> {
            // whether deleted state doc or not, continue as state doc may not exist
            deleteConfigDoc(configId, listener);
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                deleteConfigDoc(configId, listener);
            } else {
                LOG.error("Failed to delete state", exception);
                listener.onFailure(exception);
            }
        }));
    }

    private void deleteConfigDoc(String configId, ActionListener<DeleteResponse> listener) {
        LOG.info("Delete config {}", configId);
        DeleteRequest deleteRequest = new DeleteRequest(configIndexName, configId).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.delete(deleteRequest, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                listener.onResponse(deleteResponse);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void getJob(String configId, ActionListener<DeleteResponse> listener, ExecutorFunction function) {
        if (clusterService.state().metadata().indices().containsKey(CommonName.JOB_INDEX)) {
            GetRequest request = new GetRequest(CommonName.JOB_INDEX).id(configId);
            client.get(request, ActionListener.wrap(response -> onGetJobResponseForWrite(response, listener, function), exception -> {
                LOG.error("Fail to get job: " + configId, exception);
                listener.onFailure(exception);
            }));
        } else {
            function.execute();
        }
    }

    private void onGetJobResponseForWrite(GetResponse response, ActionListener<DeleteResponse> listener, ExecutorFunction function)
        throws IOException {
        if (response.isExists()) {
            String jobId = response.getId();
            if (jobId != null) {
                // check if job is running on the config, if yes, we can't delete the config
                try (
                    XContentParser parser = RestHandlerUtils
                        .createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    Job adJob = Job.parse(parser);
                    if (adJob.isEnabled()) {
                        listener.onFailure(new OpenSearchStatusException("Job is running: " + jobId, RestStatus.BAD_REQUEST));
                    } else {
                        function.execute();
                    }
                } catch (IOException e) {
                    String message = "Failed to parse job " + jobId;
                    LOG.error(message, e);
                    function.execute();
                }
            }
        } else {
            function.execute();
        }
    }
}
