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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.util.Random;

import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ml.ADColdStart;
import org.opensearch.ad.ml.ADInferencer;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Provider;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.ratelimit.CheckpointReadWorker;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * a queue for loading model checkpoint. The read is a multi-get query. Possible results are:
 * a). If a checkpoint is not found, we forward that request to the cold start queue.
 * b). When a request gets errors, the queue does not change its expiry time and puts
 *  that request to the end of the queue and automatically retries them before they expire.
 * c) When a checkpoint is found, we load that point to memory and score the input
 * data point and save the result if a complete model exists. Otherwise, we enqueue
 * the sample. If we can host that model in memory (e.g., there is enough memory),
 * we put the loaded model to cache. Otherwise (e.g., a cold entity), we write the
 * updated checkpoint back to disk.
 *
 */
public class ADCheckpointReadWorker extends
    CheckpointReadWorker<ThresholdedRandomCutForest, AnomalyResult, ThresholdingResult, ADIndex, ADIndexManagement, ADCheckpointDao, ADCheckpointWriteWorker, ADColdStart, ADModelManager, ADPriorityCache, ADSaveResultStrategy, ADColdStartWorker, ADInferencer> {
    public static final String WORKER_NAME = "ad-checkpoint-read";

    public ADCheckpointReadWorker(
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
        ADModelManager modelManager,
        ADCheckpointDao checkpointDao,
        ADColdStartWorker entityColdStartQueue,
        NodeStateManager stateManager,
        Provider<ADPriorityCache> cacheProvider,
        Duration stateTtl,
        ADCheckpointWriteWorker checkpointWriteQueue,
        ADInferencer inferencer
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
            executionTtl,
            modelManager,
            checkpointDao,
            entityColdStartQueue,
            stateManager,
            cacheProvider,
            stateTtl,
            checkpointWriteQueue,
            AD_CHECKPOINT_READ_QUEUE_CONCURRENCY,
            AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE,
            ADCommonName.CHECKPOINT_INDEX_NAME,
            AnalysisType.AD,
            inferencer
        );
    }
}
