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

/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.ratelimit;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.COOLDOWN_MINUTES;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.ExpiringState;
import org.opensearch.ad.MaintenanceState;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;

/**
 * HCAD can bombard ES with “thundering herd” traffic, in which many entities
 * make requests that need similar ES reads/writes at approximately the same
 * time. To remedy this issue we queue the requests and ensure that only a
 * limited set of requests are out for ES reads/writes.
 *
 * @param <RequestType> Individual request type that is a subtype of ADRequest
 */
public abstract class RateLimitedQueue<RequestType extends QueuedRequest> implements MaintenanceState {
    /**
     * Each request is associated with a segment. That is, a queue consists of segments.
     * Segments have their corresponding priorities: HIGH, MEDIUM, and LOW. An example
     * of HIGH priority requests is anomaly results with errors or its anomaly grade
     * larger than zero. An example of MEDIUM priority requests is a cold start request
     * for an entity. An example of LOW priority requests is checkpoint write requests
     * for a cold entity. LOW priority requests have the slightest chance to be selected
     * to be executed. MEDIUM and HIGH priority requests have higher stakes. LOW priority
     * requests have higher chances of being deleted when the size of the queue reaches
     * beyond a limit compared to MEDIUM/HIGH priority requests.
     *
     */
    class Segment implements ExpiringState {
        // last access time
        private Instant lastAccessTime;
        // data structure to hold requests
        private BlockingQueue<RequestType> content;

        Segment() {
            this.lastAccessTime = clock.instant();
            this.content = new LinkedBlockingQueue<RequestType>();
        }

        @Override
        public boolean expired(Duration stateTtl) {
            return expired(lastAccessTime, stateTtl, clock.instant());
        }

        public void put(RequestType request) throws InterruptedException {
            this.content.put(request);
        }

        public int size() {
            return this.content.size();
        }

        public void setLastAccessTime(Instant lastAccessTime) {
            this.lastAccessTime = lastAccessTime;
        }
    }

    private static final Logger LOG = LogManager.getLogger(RateLimitedQueue.class);

    protected int queueSize;
    protected final String queueName;
    private final long heapSize;
    private final int singleRequestSize;
    private float maxHeapPercentForQueue;

    // map from segment Id to its segment.
    // For high priority requests, the segment id is SegmentPriority.HIGH.name().
    // For low priority requests, the segment id is SegmentPriority.LOW.name().
    // For medium priority requests, the segment id is detector id. The objective
    // is to separate requests from different detectors and fairly process requests
    // from each detector.
    protected final ConcurrentSkipListMap<String, Segment> requestSegments;
    private String lastSelectedSegmentId;
    protected Random random;
    private ADCircuitBreakerService adCircuitBreakerService;
    protected ThreadPool threadPool;
    protected Instant cooldownStart;
    protected int coolDownMinutes;
    private float maxQueuedTaskRatio;
    protected Clock clock;
    private float mediumSegmentPruneRatio;
    private float lowSegmentPruneRatio;
    protected int maintenanceFreqConstant;
    private final Duration stateTtl;
    protected final NodeStateManager nodeStateManager;

    public RateLimitedQueue(
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
        Duration stateTtl,
        NodeStateManager nodeStateManager
    ) {
        this.heapSize = heapSizeInBytes;
        this.singleRequestSize = singleRequestSizeInBytes;
        this.maxHeapPercentForQueue = maxHeapPercentForQueueSetting.get(settings);
        this.queueSize = (int) (heapSizeInBytes * maxHeapPercentForQueue / singleRequestSizeInBytes);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(maxHeapPercentForQueueSetting, it -> {
            int oldQueueSize = queueSize;
            this.queueSize = (int) (this.heapSize * maxHeapPercentForQueue / this.singleRequestSize);
            LOG.info(new ParameterizedMessage("Queue size changed from [{}] to [{}]", oldQueueSize, queueSize));
        });

        this.queueName = queueName;
        this.random = random;
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.threadPool = threadPool;
        this.maxQueuedTaskRatio = maxQueuedTaskRatio;
        this.clock = clock;
        this.mediumSegmentPruneRatio = mediumSegmentPruneRatio;
        this.lowSegmentPruneRatio = lowSegmentPruneRatio;

        this.lastSelectedSegmentId = null;
        this.requestSegments = new ConcurrentSkipListMap<>();
        this.cooldownStart = Instant.MIN;
        this.coolDownMinutes = (int) (COOLDOWN_MINUTES.get(settings).getMinutes());
        this.maintenanceFreqConstant = maintenanceFreqConstant;
        this.stateTtl = stateTtl;
        this.nodeStateManager = nodeStateManager;
    }

    protected String getQueueName() {
        return queueName;
    }

    /**
     * To add fairness to multiple detectors, HCAD allocates queues at a per
     * detector granularity and pulls off requests across similar queues in a
     * round-robin fashion.  This way, if one detector has a much higher
     * cardinality than other detectors,  the unfinished portion of that
     * detector’s workload times out, and other detectors’ workloads continue
     * operating with predictable performance. For example, for loading checkpoints,
     * HCAD pulls off 10 requests from one detector’ queues, issues a mget request
     * to ES, wait for it to finish, and then does it again for other detectors’
     * queues.  If one queue does not have more than 10 requests, HCAD dequeues
     * the next batches of messages in the round-robin schedule.
     * @return next queue to fetch requests
     */
    protected Optional<BlockingQueue<RequestType>> selectNextQueue() {
        if (true == requestSegments.isEmpty()) {
            return Optional.empty();
        }

        String startId = lastSelectedSegmentId;
        try {
            for (int i = 0; i < requestSegments.size(); i++) {
                if (startId == null || requestSegments.size() == 1 || startId.equals(requestSegments.lastKey())) {
                    startId = requestSegments.firstKey();
                } else {
                    startId = requestSegments.higherKey(startId);
                }

                if (startId.equals(SegmentPriority.LOW.name())) {
                    continue;
                }

                Segment segment = requestSegments.get(startId);
                if (segment == null) {
                    continue;
                }

                BlockingQueue<RequestType> requests = segment.content;

                if (false == requests.isEmpty()) {
                    clearExpiredRequests(requests);

                    if (false == requests.isEmpty()) {
                        return Optional.of(requests);
                    }
                }
            }

            Segment segment = requestSegments.get(SegmentPriority.LOW.name());

            if (segment != null) {
                BlockingQueue<RequestType> requests = segment.content;
                if (requests != null && false == requests.isEmpty()) {
                    clearExpiredRequests(requests);
                    if (false == requests.isEmpty()) {
                        return Optional.of(requests);
                    }
                }
            }
            // if we haven't find a non-empty queue , return empty.
            return Optional.empty();
        } finally {
            // it is fine we may have race conditions. We are not trying to
            // be precise. The objective is to select each segment with equal probability.
            lastSelectedSegmentId = startId;
        }
    }

    private void clearExpiredRequests(BlockingQueue<RequestType> requests) {
        // In terms of request duration, HCAD throws a request out if it
        // is older than the detector frequency. This duration limit frees
        // up HCAD to work on newer requests in the subsequent detection
        // interval instead of piling up requests that no longer matter.
        // For example, loading model checkpoints for cache misses requires
        // a queue configured in front of it. A request contains the checkpoint
        // document Id and the expiry time, and the queue can hold a considerable
        // volume of such requests since the size of the request is small.
        // The expiry time is the start timestamp of the next detector run.
        // Enforcing the expiry time places an upper bound on each request’s
        // lifetime.
        RequestType head = requests.peek();
        while (head != null && head.getExpirationEpochMs() < clock.millis()) {
            requests.poll();
            head = requests.peek();
        }
    }

    protected void putOnly(RequestType request) {
        try {
            Segment requestQueue = requestSegments
                .computeIfAbsent(
                    SegmentPriority.MEDIUM == request.getPriority() ? request.getDetectorId() : request.getPriority().name(),
                    k -> new Segment()
                );

            requestQueue.setLastAccessTime(clock.instant());
            requestQueue.put(request);
        } catch (Exception e) {
            LOG.error(new ParameterizedMessage("Failed to add requests to [{}]", this.queueName), e);
        }
    }

    private void maintainForThreadPool() {
        for (final ThreadPoolStats.Stats stats : threadPool.stats()) {
            String name = stats.getName();
            // cold entity queue mostly use these 3 threadpools
            if (ThreadPool.Names.SEARCH.equals(name) || ThreadPool.Names.GET.equals(name) || ThreadPool.Names.WRITE.equals(name)) {
                if (stats.getQueue() > (int) (maxQueuedTaskRatio * threadPool.info(name).getQueueSize().singles())) {
                    setCoolDownStart();
                    break;
                }
            }
        }
    }

    private void prune(Map<String, Segment> segments) {
        for (Map.Entry<String, Segment> segmentEntry : segments.entrySet()) {
            if (segmentEntry.getKey().equals(SegmentPriority.HIGH.name())) {
                return;
            }
            // remove more requests in the low priority segment
            float removeRatio = mediumSegmentPruneRatio;
            if (segmentEntry.getKey().equals(SegmentPriority.LOW.name())) {
                removeRatio = lowSegmentPruneRatio;
            }

            Segment segment = segmentEntry.getValue();

            if (segment == null) {
                return;
            }

            BlockingQueue<RequestType> segmentContent = segment.content;
            // remove 10% of old requests
            int deletedRequests = (int) (segmentContent.size() * removeRatio);
            while (false == segmentContent.isEmpty() && deletedRequests-- >= 0) {
                segmentContent.poll();
            }
        }
    }

    private void maintainForMemory() {
        // removed expired segment
        maintenance(requestSegments, stateTtl);

        if (isSizeExceeded()) {
            // remove until reaching below queueSize
            do {
                prune(requestSegments);
            } while (isSizeExceeded());
        } else if (adCircuitBreakerService.isOpen()) {
            // remove a few items in each segment
            prune(requestSegments);
        }
    }

    private boolean isSizeExceeded() {
        Collection<Segment> segments = requestSegments.values();
        int totalSize = 0;

        // When faced with a backlog beyond the limit, we prefer fresh requests
        // and throws away old requests.
        // release space so that put won't block
        for (Segment segment : segments) {
            totalSize += segment.size();
        }
        return totalSize >= queueSize;
    }

    @Override
    public void maintenance() {
        try {
            maintainForMemory();
            maintainForThreadPool();
        } catch (Exception e) {
            // it is normal to maintain why other
            LOG.warn("Failed to maintain", e);
        }
    }

    /**
     * Start cooldown during a overloaded situation
     */
    protected void setCoolDownStart() {
        cooldownStart = clock.instant();
    }

    /**
     * @param batchSize the max number of requests to fetch
     * @return a list of batchSize requests (can be less)
     */
    protected List<RequestType> getRequests(int batchSize) {
        List<RequestType> toProcess = new ArrayList<>(batchSize);

        Set<BlockingQueue<RequestType>> selectedQueue = new HashSet<>();

        while (toProcess.size() < batchSize) {
            Optional<BlockingQueue<RequestType>> queue = selectNextQueue();
            if (false == queue.isPresent()) {
                // no queue has requests
                break;
            }

            BlockingQueue<RequestType> nextToProcess = queue.get();
            if (selectedQueue.contains(nextToProcess)) {
                // we have gone around all of the queues
                break;
            }
            selectedQueue.add(nextToProcess);

            List<RequestType> requests = new ArrayList<>();
            // concurrent requests will wait to prevent concurrent draining.
            // This is fine since the operation is fast
            nextToProcess.drainTo(requests, batchSize);
            toProcess.addAll(requests);
        }

        return toProcess;
    }

    /**
     * Enqueuing runs asynchronously: we put requests in a queue, try to execute
     * them. The thread executing requests won't block the thread inserting
     * requests to the queue.
     * @param request Individual request
     */
    public void put(RequestType request) {
        if (request == null) {
            return;
        }
        putOnly(request);

        process();
    }

    public void putAll(List<RequestType> requests) {
        if (requests == null || requests.isEmpty()) {
            return;
        }
        try {
            for (RequestType request : requests) {
                putOnly(request);
            }

            process();
        } catch (Exception e) {
            LOG.error(new ParameterizedMessage("Failed to add requests to [{}]", getQueueName()), e);
        }
    }

    protected void process() {
        if (random.nextInt(maintenanceFreqConstant) == 1) {
            maintenance();
        }

        // still in cooldown period
        if (cooldownStart.plus(Duration.ofMinutes(coolDownMinutes)).isAfter(clock.instant())) {
            threadPool.schedule(() -> {
                try {
                    process();
                } catch (Exception e) {
                    LOG.error(new ParameterizedMessage("Fail to process requests in [{}].", this.queueName), e);
                }
            }, new TimeValue(coolDownMinutes, TimeUnit.MINUTES), AnomalyDetectorPlugin.AD_THREAD_POOL_NAME);
        } else {
            try {
                triggerProcess();
            } catch (Exception e) {
                LOG.error(String.format(Locale.ROOT, "Failed to process requests from %s", getQueueName()), e);
                if (e != null && e instanceof AnomalyDetectionException) {
                    AnomalyDetectionException adExep = (AnomalyDetectionException) e;
                    nodeStateManager.setException(adExep.getAnomalyDetectorId(), adExep);
                }
            }

        }
    }

    /**
     * How to execute requests is abstracted out and left to RateLimitedQueue's subclasses to implement.
     */
    protected abstract void triggerProcess();
}
