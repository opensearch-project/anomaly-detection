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
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.ratelimit.ADColdEntityWorker;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;

public abstract class ScheduledWorker<RequestType extends QueuedRequest, TransformedRequestType extends QueuedRequest> extends
    RateLimitedRequestWorker<RequestType> {
    private static final Logger LOG = LogManager.getLogger(ADColdEntityWorker.class);

    // the number of requests forwarded to the target queue
    protected volatile int batchSize;
    private final RateLimitedRequestWorker<TransformedRequestType> targetQueue;
    // indicate whether a future pull over cold entity queues is scheduled
    private boolean scheduled;
    protected volatile int expectedExecutionTimeInMilliSecsPerRequest;

    public ScheduledWorker(
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
        RateLimitedRequestWorker<TransformedRequestType> targetQueue,
        Duration stateTtl,
        NodeStateManager nodeStateManager,
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
            stateTtl,
            nodeStateManager,
            context
        );

        this.targetQueue = targetQueue;
        this.scheduled = false;
    }

    private void pullRequests() {
        int pulledRequestSize = 0;
        int filteredRequestSize = 0;
        try {
            List<RequestType> requests = getRequests(batchSize);
            if (requests == null || requests.isEmpty()) {
                return;
            }
            pulledRequestSize = requests.size();
            List<TransformedRequestType> filteredRequests = transformRequests(requests);
            if (!filteredRequests.isEmpty()) {
                targetQueue.putAll(filteredRequests);
                filteredRequestSize = filteredRequests.size();
            }
        } catch (Exception e) {
            LOG.error("Error enqueuing cold entity requests", e);
        } finally {
            if (pulledRequestSize < batchSize) {
                scheduled = false;
            } else {
                // there might be more to fetch
                // schedule a pull from queue every few seconds.
                scheduled = true;
                if (filteredRequestSize == 0) {
                    pullRequests();
                } else {
                    schedulePulling(getScheduleDelay(filteredRequestSize));
                }
            }
        }
    }

    private synchronized void schedulePulling(TimeValue delay) {
        try {
            threadPool.schedule(this::pullRequests, delay, threadPoolName);
        } catch (Exception e) {
            LOG.error("Fail to schedule cold entity pulling", e);
        }
    }

    /**
     * only pull requests to process when there's no other scheduled run
     */
    @Override
    protected void triggerProcess() {
        if (false == scheduled) {
            pullRequests();
        }
    }

    /**
     * The method calculates the delay we have to set to control the rate of cold
     * entity processing. We wait longer if the requestSize is larger to give the
     * system more time to processing requests.
     * @param requestSize requests to process
     * @return the delay for the next scheduled run
     */
    private TimeValue getScheduleDelay(int requestSize) {
        return TimeValue.timeValueMillis(requestSize * expectedExecutionTimeInMilliSecsPerRequest);
    }

    /**
     * Transform requests before forwarding to another queue
     * @param requests requests to be transformed
     *
     * @return processed requests
     */
    protected abstract List<TransformedRequestType> transformRequests(List<RequestType> requests);
}
