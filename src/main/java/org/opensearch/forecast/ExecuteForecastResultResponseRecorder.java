/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;

import org.opensearch.commons.authuser.User;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.forecast.transport.ForecastProfileAction;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.ExecuteResultResponseRecorder;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.ResultResponse;
import org.opensearch.timeseries.transport.handler.ResultBulkIndexingHandler;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.client.Client;

public class ExecuteForecastResultResponseRecorder extends
    ExecuteResultResponseRecorder<ForecastIndex, ForecastIndexManagement, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastTaskManager, ForecastResult, ForecastProfileAction> {

    public ExecuteForecastResultResponseRecorder(
        ForecastIndexManagement indexManagement,
        ResultBulkIndexingHandler<ForecastResult, ForecastIndex, ForecastIndexManagement> resultHandler,
        ForecastTaskManager taskManager,
        DiscoveryNodeFilterer nodeFilter,
        ThreadPool threadPool,
        Client client,
        NodeStateManager nodeStateManager,
        TaskCacheManager taskCacheManager,
        int rcfMinSamples
    ) {
        super(
            indexManagement,
            resultHandler,
            taskManager,
            nodeFilter,
            threadPool,
            TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME,
            client,
            nodeStateManager,
            taskCacheManager,
            rcfMinSamples,
            ForecastIndex.RESULT,
            AnalysisType.FORECAST,
            ForecastProfileAction.INSTANCE
        );
    }

    @Override
    protected ForecastResult createErrorResult(
        String configId,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executeEndTime,
        String errorMessage,
        User user
    ) {
        return new ForecastResult(
            configId,
            null, // no task id
            new ArrayList<FeatureData>(),
            dataStartTime,
            dataEndTime,
            executeEndTime,
            Instant.now(),
            errorMessage,
            Optional.empty(), // single-stream forecasters have no entity
            user,
            indexManagement.getSchemaVersion(resultIndex)
        );
    }

    @Override
    protected void updateRealtimeTask(ResultResponse<ForecastResult> response, String configId) {
        if (taskManager.skipUpdateRealtimeTask(configId, response.getError())) {
            return;
        }

        delayedUpdate(response, configId);
    }
}
