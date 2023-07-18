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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;

/**
 * A queue to run concurrent requests (either batch or single request).
 * The concurrency is configurable. The callers use the put method to put requests
 * in and the queue tries to execute them if there are concurrency slots.
 *
 * @param <RequestType> Individual request type that is a subtype of ADRequest
 */
public abstract class ConcurrentWorker<RequestType extends QueuedRequest> extends RateLimitedRequestWorker<RequestType> {
    private static final Logger LOG = LogManager.getLogger(ConcurrentWorker.class);

    private Semaphore permits;

    private Instant lastExecuteTime;
    private Duration executionTtl;

    /**
     *
     * Constructor with dependencies and configuration.
     *
     * @param queueName queue's name
     * @param heapSizeInBytes ES heap size
     * @param singleRequestSizeInBytes single request's size in bytes
     * @param maxHeapPercentForQueueSetting max heap size used for the queue. Used for
     *   rate AD's usage on ES threadpools.
     * @param clusterService Cluster service accessor
     * @param random Random number generator
     * @param adCircuitBreakerService AD Circuit breaker service
     * @param threadPool threadpool accessor
     * @param settings Cluster settings getter
     * @param maxQueuedTaskRatio maximum queued tasks ratio in ES threadpools
     * @param clock Clock to get current time
     * @param mediumSegmentPruneRatio the percent of medium priority requests to prune when the queue is full
     * @param lowSegmentPruneRatio the percent of low priority requests to prune when the queue is full
     * @param maintenanceFreqConstant a constant help define the frequency of maintenance.  We cannot do
     *   the expensive maintenance too often.
     * @param concurrencySetting Max concurrent processing of the queued events
     * @param executionTtl Max execution time of a single request
     * @param stateTtl max idle state duration.  Used to clean unused states.
     * @param nodeStateManager node state accessor
     */
    public ConcurrentWorker(
        String queueName,
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
        Setting<Integer> concurrencySetting,
        Duration executionTtl,
        Duration stateTtl,
        NodeStateManager nodeStateManager
    ) {
        super(
            queueName,
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
            stateTtl,
            nodeStateManager
        );

        this.permits = new Semaphore(concurrencySetting.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(concurrencySetting, it -> permits = new Semaphore(it));

        this.lastExecuteTime = clock.instant();
        this.executionTtl = executionTtl;
    }

    @Override
    public void maintenance() {
        super.maintenance();

        if (lastExecuteTime.plus(executionTtl).isBefore(clock.instant()) && permits.availablePermits() == 0 && false == isQueueEmpty()) {
            LOG.warn("previous execution has been running for too long.  Maybe there are bugs.");

            // Release one permit. This is a stop gap solution as I don't know
            // whether the system is under heavy workload or not. Release multiple
            // permits might cause the situation even worse. So I am conservative here.
            permits.release();
        }
    }

    /**
     * try to execute queued requests if there are concurrency slots and return right away.
     */
    @Override
    protected void triggerProcess() {
        threadPool.executor(TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME).execute(() -> {
            if (permits.tryAcquire()) {
                try {
                    lastExecuteTime = clock.instant();
                    execute(() -> {
                        permits.release();
                        process();
                    }, () -> { permits.release(); });
                } catch (Exception e) {
                    permits.release();
                    // throw to the root level to catch
                    throw e;
                }
            }
        });
    }

    /**
     * Execute requests in toProcess.  The implementation needs to call cleanUp after done.
     * The 1st callback is executed after processing one request. So we keep looking for
     * new requests if there is any after finishing one request. Otherwise, just release
     * (the 2nd callback) without calling process.
     * @param afterProcessCallback callback after processing requests
     * @param emptyQueueCallback callback for empty queues
     */
    protected abstract void execute(Runnable afterProcessCallback, Runnable emptyQueueCallback);
}
