/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest.handler;

import static org.opensearch.forecast.model.ForecastTaskType.RUN_ONCE_TASK_TYPES;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_REQUEST_TIMEOUT;

import java.util.List;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.ExecuteForecastResultResponseRecorder;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.forecast.transport.ForecastProfileAction;
import org.opensearch.forecast.transport.ForecastResultAction;
import org.opensearch.forecast.transport.ForecastResultRequest;
import org.opensearch.forecast.transport.StopForecasterAction;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.rest.handler.IndexJobActionHandler;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.transport.ResultRequest;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class ForecastIndexJobActionHandler extends
    IndexJobActionHandler<ForecastIndex, ForecastIndexManagement, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastTaskManager, ForecastResult, ForecastProfileAction, ExecuteForecastResultResponseRecorder> {

    public ForecastIndexJobActionHandler(
        Client client,
        ForecastIndexManagement indexManagement,
        NamedXContentRegistry xContentRegistry,
        ForecastTaskManager adTaskManager,
        ExecuteForecastResultResponseRecorder recorder,
        NodeStateManager nodeStateManager,
        Settings settings
    ) {
        super(
            client,
            indexManagement,
            xContentRegistry,
            adTaskManager,
            recorder,
            ForecastResultAction.INSTANCE,
            AnalysisType.FORECAST,
            ForecastIndex.STATE.getIndexName(),
            StopForecasterAction.INSTANCE,
            nodeStateManager,
            settings,
            FORECAST_REQUEST_TIMEOUT
        );
    }

    @Override
    protected ResultRequest createResultRequest(String configID, long start, long end) {
        return new ForecastResultRequest(configID, start, end);
    }

    @Override
    protected List<ForecastTaskType> getBatchConfigTaskTypes() {
        return RUN_ONCE_TASK_TYPES;
    }

    /**
     * Stop config.
     * For realtime, will set job as disabled.
     * For run once, will set its task as inactive.
     *
     * @param configId config id
     * @param historical stop historical analysis or not
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    @Override
    public void stopConfig(
        String configId,
        boolean historical,
        User user,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        // make sure forecaster exists
        nodeStateManager.getConfig(configId, AnalysisType.FORECAST, (config) -> {
            if (!config.isPresent()) {
                listener.onFailure(new OpenSearchStatusException(CommonMessages.FAIL_TO_FIND_CONFIG_MSG + configId, RestStatus.NOT_FOUND));
                return;
            }
            taskManager.getAndExecuteOnLatestConfigLevelTask(configId, ForecastTaskType.RUN_ONCE_TASK_TYPES, (task) -> {
                // stop realtime forecaster job
                stopJob(configId, transportService, listener);
            }, transportService, true, listener); // true means reset task state as inactive/stopped state
        }, listener);
    }
}
