/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.ratelimit;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_COLD_START_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Random;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.caching.ForecastPriorityCache;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ml.ForecastColdStart;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.ml.RCFCasterResult;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.ratelimit.ColdStartWorker;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.util.ParseUtils;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastColdStartWorker extends
    ColdStartWorker<RCFCaster, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastCheckpointWriteWorker, ForecastColdStart, ForecastPriorityCache, ForecastResult, RCFCasterResult, ForecastModelManager, ForecastSaveResultStrategy> {
    public static final String WORKER_NAME = "forecast-hc-cold-start";

    public ForecastColdStartWorker(
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        CircuitBreakerService circuitBreakerService,
        ThreadPool threadPool,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        Duration executionTtl,
        ForecastColdStart coldStarter,
        Duration stateTtl,
        NodeStateManager nodeStateManager,
        ForecastPriorityCache cacheProvider,
        ForecastModelManager forecastModelManager,
        ForecastSaveResultStrategy saveStrategy
    ) {
        super(
            WORKER_NAME,
            heapSizeInBytes,
            singleRequestSizeInBytes,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            FORECAST_COLD_START_QUEUE_CONCURRENCY,
            executionTtl,
            coldStarter,
            stateTtl,
            nodeStateManager,
            cacheProvider,
            AnalysisType.FORECAST,
            forecastModelManager,
            saveStrategy
        );
    }

    @Override
    protected ModelState<RCFCaster> createEmptyState(FeatureRequest coldStartRequest, String modelId, String configId) {
        return new ModelState<RCFCaster>(
            null,
            modelId,
            configId,
            ModelManager.ModelType.RCFCASTER.getName(),
            clock,
            0,
            coldStartRequest.getEntity(),
            new ArrayDeque<>()
        );
    }

    @Override
    protected ForecastResult createIndexableResult(Config config, String taskId, String modelId, Sample entry, Optional<Entity> entity) {
        return new ForecastResult(
            config.getId(),
            taskId,
            ParseUtils.getFeatureData(entry.getValueList(), config),
            entry.getDataStartTime(),
            entry.getDataEndTime(),
            Instant.now(),
            Instant.now(),
            "",
            entity,
            config.getUser(),
            config.getSchemaVersion()
        );
    }
}
