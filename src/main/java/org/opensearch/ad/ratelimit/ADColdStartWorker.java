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

package org.opensearch.ad.ratelimit;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_ENTITY_COLD_START_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Random;

import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ml.ADColdStart;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
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

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * A queue for HCAD model training (a.k.a. cold start). As model training is a
 * pretty expensive operation, we pull cold start requests from the queue in a
 * serial fashion. Each detector has an equal chance of being pulled. The equal
 * probability is achieved by putting model training requests for different
 * detectors into different segments and pulling requests from segments in a
 * round-robin fashion.
 *
 */

// suppress warning due to the use of generic type ModelState
public class ADColdStartWorker extends
    ColdStartWorker<ThresholdedRandomCutForest, ADIndex, ADIndexManagement, ADCheckpointDao, ADCheckpointWriteWorker, ADColdStart, ADPriorityCache, AnomalyResult, ThresholdingResult, ADModelManager, ADSaveResultStrategy> {
    public static final String WORKER_NAME = "ad-cold-start";

    public ADColdStartWorker(
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
        ADColdStart entityColdStarter,
        Duration stateTtl,
        NodeStateManager nodeStateManager,
        ADPriorityCache cacheProvider,
        ADModelManager modelManager,
        ADSaveResultStrategy saveStrategy
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
            TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            AD_ENTITY_COLD_START_QUEUE_CONCURRENCY,
            executionTtl,
            entityColdStarter,
            stateTtl,
            nodeStateManager,
            cacheProvider,
            AnalysisType.AD,
            modelManager,
            saveStrategy
        );
    }

    @Override
    protected ModelState<ThresholdedRandomCutForest> createEmptyState(FeatureRequest request, String modelId, String configId) {
        return new ModelState<ThresholdedRandomCutForest>(
            null,
            modelId,
            configId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            0,
            request.getEntity(),
            new ArrayDeque<>()
        );
    }

    @Override
    protected AnomalyResult createIndexableResult(Config config, String taskId, String modelId, Sample entry, Optional<Entity> entity) {
        return new AnomalyResult(
            config.getId(),
            taskId,
            Double.NaN,
            Double.NaN,
            Double.NaN,
            ParseUtils.getFeatureData(entry.getValueList(), config),
            entry.getDataStartTime(),
            entry.getDataEndTime(),
            Instant.now(),
            Instant.now(),
            "",
            entity,
            config.getUser(),
            config.getSchemaVersion(),
            modelId,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }
}
