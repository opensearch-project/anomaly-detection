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
import static org.opensearch.ad.settings.AnomalyDetectorSettings.EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

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
public class ColdEntityWorker extends ScheduledWorker<EntityFeatureRequest, EntityFeatureRequest> {
    public static final String WORKER_NAME = "cold-entity";

    public ColdEntityWorker(
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        ADCircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        CheckpointReadWorker checkpointReadQueue,
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
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            checkpointReadQueue,
            stateTtl,
            nodeStateManager
        );

        this.batchSize = AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE, it -> this.batchSize = it);

        this.expectedExecutionTimeInMilliSecsPerRequest = AnomalyDetectorSettings.EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS
            .get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(
                EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS,
                it -> this.expectedExecutionTimeInMilliSecsPerRequest = it
            );
    }

    @Override
    protected List<EntityFeatureRequest> transformRequests(List<EntityFeatureRequest> requests) {
        // guarantee we only send low priority requests
        return requests.stream().filter(request -> request.priority == RequestPriority.LOW).collect(Collectors.toList());
    }
}
