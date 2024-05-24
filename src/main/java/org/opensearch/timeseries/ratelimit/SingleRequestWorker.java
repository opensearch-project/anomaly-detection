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
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;

public abstract class SingleRequestWorker<RequestType extends QueuedRequest> extends ConcurrentWorker<RequestType> {
    private static final Logger LOG = LogManager.getLogger(SingleRequestWorker.class);

    public SingleRequestWorker(
        String queueName,
        long heapSize,
        int singleRequestSize,
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
        Setting<Integer> concurrencySetting,
        Duration executionTtl,
        Duration stateTtl,
        NodeStateManager nodeStateManager,
        AnalysisType context
    ) {
        super(
            queueName,
            heapSize,
            singleRequestSize,
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
            concurrencySetting,
            executionTtl,
            stateTtl,
            nodeStateManager,
            context
        );
    }

    @Override
    protected void execute(Runnable afterProcessCallback, Runnable emptyQueueCallback) {
        RequestType request = null;

        Optional<BlockingQueue<RequestType>> queueOptional = selectNextQueue();
        if (false == queueOptional.isPresent()) {
            // no queue has requests
            emptyQueueCallback.run();
            return;
        }

        BlockingQueue<RequestType> queue = queueOptional.get();
        if (false == queue.isEmpty()) {
            request = queue.poll();
        }

        if (request == null) {
            emptyQueueCallback.run();
            return;
        }

        final ActionListener<Void> handlerWithRelease = ActionListener.wrap(afterProcessCallback);
        executeRequest(request, handlerWithRelease);
    }

    /**
     * Used by subclasses to creates customized logic to send batch requests.
     * After everything finishes, the method should call listener.
     * @param request request to execute
     * @param listener customized listener
     */
    protected abstract void executeRequest(RequestType request, ActionListener<Void> listener);
}
