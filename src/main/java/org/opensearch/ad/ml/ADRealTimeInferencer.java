/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.ml;

import static org.opensearch.timeseries.TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME;

import java.time.Clock;

import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.ad.ratelimit.ADColdStartWorker;
import org.opensearch.ad.ratelimit.ADSaveResultStrategy;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.ml.RealTimeInferencer;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.Stats;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class ADRealTimeInferencer extends
    RealTimeInferencer<ThresholdedRandomCutForest, AnomalyResult, ThresholdingResult, ADIndex, ADIndexManagement, ADCheckpointDao, ADCheckpointWriteWorker, ADColdStart, ADModelManager, ADSaveResultStrategy, ADPriorityCache, ADTaskCacheManager, ADTaskType, ADTask, ADTaskManager, ADColdStartWorker> {

    public ADRealTimeInferencer(
        ADModelManager modelManager,
        Stats stats,
        ADCheckpointDao checkpointDao,
        ADColdStartWorker coldStartWorker,
        ADSaveResultStrategy resultWriteWorker,
        ADCacheProvider cache,
        ThreadPool threadPool,
        Clock clock,
        SearchFeatureDao searchFeatureDao
    ) {
        super(
            modelManager,
            stats,
            StatNames.AD_MODEL_CORRUTPION_COUNT.getName(),
            checkpointDao,
            coldStartWorker,
            resultWriteWorker,
            cache,
            threadPool,
            AD_THREAD_POOL_NAME,
            clock,
            searchFeatureDao,
            AnalysisType.AD
        );
    }

}
