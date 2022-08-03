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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_BATCH_SIZE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

public class CheckpointMaintainWorker extends ScheduledWorker<CheckpointMaintainRequest, CheckpointWriteRequest> {
    private static final Logger LOG = LogManager.getLogger(CheckpointMaintainWorker.class);
    public static final String WORKER_NAME = "checkpoint-maintain";

    private CheckPointMaintainRequestAdapter adapter;

    public CheckpointMaintainWorker(
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
        CheckpointWriteWorker checkpointWriteQueue,
        Duration stateTtl,
        NodeStateManager nodeStateManager,
        CheckPointMaintainRequestAdapter adapter
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
            checkpointWriteQueue,
            stateTtl,
            nodeStateManager
        );

        this.batchSize = CHECKPOINT_WRITE_QUEUE_BATCH_SIZE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CHECKPOINT_WRITE_QUEUE_BATCH_SIZE, it -> this.batchSize = it);

        this.expectedExecutionTimeInMilliSecsPerRequest = AnomalyDetectorSettings.EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS
            .get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(
                EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS,
                it -> this.expectedExecutionTimeInMilliSecsPerRequest = it
            );
        this.adapter = adapter;
    }

    @Override
    protected List<CheckpointWriteRequest> transformRequests(List<CheckpointMaintainRequest> requests) {
        List<CheckpointWriteRequest> allRequests = new ArrayList<>();
        for (CheckpointMaintainRequest request : requests) {
            Optional<CheckpointWriteRequest> converted = adapter.convert(request);
            if (!converted.isEmpty()) {
                allRequests.add(converted.get());
            }
        }
        return allRequests;
    }
}
