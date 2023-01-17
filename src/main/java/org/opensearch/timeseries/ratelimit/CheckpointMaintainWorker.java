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

package org.opensearch.timeseries.ratelimit;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;

public abstract class CheckpointMaintainWorker extends ScheduledWorker<CheckpointMaintainRequest, CheckpointWriteRequest> {

    private Function<CheckpointMaintainRequest, Optional<CheckpointWriteRequest>> converter;

    public CheckpointMaintainWorker(
        String workerName,
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        CircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        String threadPoolName,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        RateLimitedRequestWorker<CheckpointWriteRequest> targetQueue,
        Duration stateTtl,
        NodeStateManager nodeStateManager,
        Function<CheckpointMaintainRequest, Optional<CheckpointWriteRequest>> converter,
        AnalysisType context
    ) {
        super(
            workerName,
            heapSizeInBytes,
            singleRequestSizeInBytes,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            threadPoolName,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            targetQueue,
            stateTtl,
            nodeStateManager,
            context
        );
        this.converter = converter;
    }

    @Override
    protected List<CheckpointWriteRequest> transformRequests(List<CheckpointMaintainRequest> requests) {
        List<CheckpointWriteRequest> allRequests = new ArrayList<>();
        for (CheckpointMaintainRequest request : requests) {
            Optional<CheckpointWriteRequest> converted = converter.apply(request);
            if (!converted.isEmpty()) {
                allRequests.add(converted.get());
            }
        }
        return allRequests;
    }
}
