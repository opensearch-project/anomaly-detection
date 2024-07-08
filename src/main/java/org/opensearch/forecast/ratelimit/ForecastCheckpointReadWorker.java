/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.ratelimit;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_CHECKPOINT_READ_QUEUE_BATCH_SIZE;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_CHECKPOINT_READ_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.util.Random;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Provider;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.caching.ForecastPriorityCache;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ml.ForecastColdStart;
import org.opensearch.forecast.ml.ForecastInferencer;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.ml.RCFCasterResult;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.ratelimit.CheckpointReadWorker;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastCheckpointReadWorker extends
    CheckpointReadWorker<RCFCaster, ForecastResult, RCFCasterResult, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastCheckpointWriteWorker, ForecastColdStart, ForecastModelManager, ForecastPriorityCache, ForecastSaveResultStrategy, ForecastColdStartWorker, ForecastInferencer> {
    public static final String WORKER_NAME = "forecast-checkpoint-read";

    public ForecastCheckpointReadWorker(
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
        ForecastModelManager modelManager,
        ForecastCheckpointDao checkpointDao,
        ForecastColdStartWorker entityColdStartQueue,
        NodeStateManager stateManager,
        Provider<ForecastPriorityCache> cacheProvider,
        Duration stateTtl,
        ForecastCheckpointWriteWorker checkpointWriteQueue,
        ForecastInferencer inferencer
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
            executionTtl,
            modelManager,
            checkpointDao,
            entityColdStartQueue,
            stateManager,
            cacheProvider,
            stateTtl,
            checkpointWriteQueue,
            FORECAST_CHECKPOINT_READ_QUEUE_CONCURRENCY,
            FORECAST_CHECKPOINT_READ_QUEUE_BATCH_SIZE,
            ForecastCommonName.FORECAST_CHECKPOINT_INDEX_NAME,
            AnalysisType.FORECAST,
            inferencer
        );
    }
}
