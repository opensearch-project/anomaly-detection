/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.ml;

import static org.opensearch.timeseries.TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME;

import java.time.Clock;

import org.opensearch.forecast.caching.ForecastCacheProvider;
import org.opensearch.forecast.caching.ForecastPriorityCache;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.ratelimit.ForecastCheckpointWriteWorker;
import org.opensearch.forecast.ratelimit.ForecastColdStartWorker;
import org.opensearch.forecast.ratelimit.ForecastSaveResultStrategy;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.ml.RealTimeInferencer;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.task.TaskCacheManager;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastRealTimeInferencer extends
    RealTimeInferencer<RCFCaster, ForecastResult, RCFCasterResult, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastCheckpointWriteWorker, ForecastColdStart, ForecastModelManager, ForecastSaveResultStrategy, ForecastPriorityCache, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastTaskManager, ForecastColdStartWorker> {

    public ForecastRealTimeInferencer(
        ForecastModelManager modelManager,
        Stats stats,
        ForecastCheckpointDao checkpointDao,
        ForecastColdStartWorker coldStartWorker,
        ForecastSaveResultStrategy resultWriteWorker,
        ForecastCacheProvider cache,
        ThreadPool threadPool,
        Clock clock,
        SearchFeatureDao searchFeatureDao
    ) {
        super(
            modelManager,
            stats,
            StatNames.FORECAST_MODEL_CORRUPTION_COUNT.getName(),
            checkpointDao,
            coldStartWorker,
            resultWriteWorker,
            cache,
            threadPool,
            FORECAST_THREAD_POOL_NAME,
            clock,
            searchFeatureDao,
            AnalysisType.FORECAST
        );
    }

}
