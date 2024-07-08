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
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS;

import java.time.Clock;
import java.time.Duration;
import java.util.Random;

import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ml.ADColdStart;
import org.opensearch.ad.ml.ADInferencer;
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
import org.opensearch.timeseries.ratelimit.ColdEntityWorker;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * A queue slowly releasing low-priority requests to CheckpointReadQueue
 *
 * ColdEntityQueue is a queue to absorb cold entities. Like hot entities, we load a cold
 * entity's model checkpoint from disk, train models if the checkpoint is not found,
 * query for missed features to complete a shingle, use the models to check whether
 * the incoming feature is normal, update models, and save the detection results to disks. 
 * Implementation-wise, we reuse the queues we have developed for hot entities.
 * The differences are: we process hot entities as long as resources (e.g., AD
 * thread pool has availability) are available, while we release cold entity requests
 * to other queues at a slow controlled pace. Also, cold entity requests' priority is low.
 * So only when there are no hot entity requests to process are we going to process cold
 * entity requests. 
 *
 */
public class ADColdEntityWorker extends
    ColdEntityWorker<ThresholdedRandomCutForest, AnomalyResult, ADIndex, ADIndexManagement, ADCheckpointDao, ThresholdingResult, ADModelManager, ADCheckpointWriteWorker, ADColdStart, ADPriorityCache, ADSaveResultStrategy, ADColdStartWorker, ADInferencer, ADCheckpointReadWorker> {
    public static final String WORKER_NAME = "ad-cold-entity";

    public ADColdEntityWorker(
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
        ADCheckpointReadWorker checkpointReadQueue,
        Duration stateTtl,
        NodeStateManager nodeStateManager
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
            checkpointReadQueue,
            stateTtl,
            nodeStateManager,
            AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE,
            AD_EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS,
            AnalysisType.AD
        );
    }
}
