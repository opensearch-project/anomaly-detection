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

package org.opensearch.forecast.ratelimit;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_RESULT_WRITE_QUEUE_BATCH_SIZE;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_RESULT_WRITE_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Random;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.transport.ForecastResultBulkRequest;
import org.opensearch.forecast.transport.handler.ForecastIndexMemoryPressureAwareResultHandler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.ResultWriteWorker;

public class ForecastResultWriteWorker extends
    ResultWriteWorker<ForecastResult, ForecastResultWriteRequest, ForecastResultBulkRequest, ForecastIndex, ForecastIndexManagement, ForecastIndexMemoryPressureAwareResultHandler> {
    public static final String WORKER_NAME = "forecast-result-write";

    public ForecastResultWriteWorker(
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        CircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        Duration executionTtl,
        ForecastIndexMemoryPressureAwareResultHandler resultHandler,
        NamedXContentRegistry xContentRegistry,
        NodeStateManager stateManager,
        Duration stateTtl
    ) {
        super(
            WORKER_NAME,
            heapSizeInBytes,
            singleRequestSizeInBytes,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            FORECAST_RESULT_WRITE_QUEUE_CONCURRENCY,
            executionTtl,
            FORECAST_RESULT_WRITE_QUEUE_BATCH_SIZE,
            stateTtl,
            stateManager,
            resultHandler,
            xContentRegistry,
            ForecastResult::parse,
            AnalysisType.FORECAST
        );
    }

    @Override
    protected ForecastResultBulkRequest toBatchRequest(List<ForecastResultWriteRequest> toProcess) {
        final ForecastResultBulkRequest bulkRequest = new ForecastResultBulkRequest();
        for (ForecastResultWriteRequest request : toProcess) {
            bulkRequest.add(request);
        }
        return bulkRequest;
    }

    @Override
    protected ForecastResultWriteRequest createResultWriteRequest(
        long expirationEpochMs,
        String configId,
        RequestPriority priority,
        ForecastResult result,
        String resultIndex,
        boolean flattenResultIndex
    ) {
        return new ForecastResultWriteRequest(expirationEpochMs, configId, priority, result, resultIndex, flattenResultIndex);
    }
}
