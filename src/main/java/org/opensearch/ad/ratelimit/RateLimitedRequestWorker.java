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
 * HCAD can bombard Opensearch with “thundering herd” traffic, in which many entities
 * make requests that need similar Opensearch reads/writes at approximately the same
 * time. To remedy this issue we queue the requests and ensure that only a
 * limited set of requests are out for Opensearch reads/writes.
 *
 * @param <RequestType> Individual request type that is a subtype of ADRequest
 */
public abstract class RateLimitedRequestWorker<RequestType extends QueuedRequest> implements MaintenanceState {
    /**
     * Each request is associated with a RequestQueue. That is, a queue consists of RequestQueues.
     * RequestQueues have their corresponding priorities: HIGH, MEDIUM, and LOW. An example
     * of HIGH priority requests is anomaly results with errors or its anomaly grade
     * larger than zero. An example of MEDIUM priority requests is a cold start request
     * for an entity. An example of LOW priority requests is checkpoint write requests
     * for a cold entity. LOW priority requests have the slightest chance to be selected
     * to be executed. MEDIUM and HIGH priority requests have higher stakes. LOW priority
     * requests have higher chances of being deleted when the size of the queue reaches
     * beyond a limit compared to MEDIUM/HIGH priority requests.
     *
     */
    class RequestQueue implements ExpiringState {
        /*
         * last access time of the RequestQueue
         * This does not have to be precise, just a signal for unused old RequestQueue
         * that can be removed.  It is fine if we have race condition.  Don't want
         * to synchronize the access as this could penalize performance.
         */
        private Instant lastAccessTime;
        // data structure to hold requests. Cannot be reassigned. This is to
        // guarantee a RequestQueue's content cannot be null.
        private final BlockingQueue<RequestType> content;

        RequestQueue() {
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
    }

    private static final Logger LOG = LogManager.getLogger(RateLimitedRequestWorker.class);

    protected volatile int queueSize;
    protected final String queueName;
    private final long heapSize;
    private final int singleRequestSize;
    private float maxHeapPercentForQueue;

    // map from RequestQueue Id to its RequestQueue.
    // For high priority requests, the RequestQueue id is RequestPriority.HIGH.name().
    // For low priority requests, the RequestQueue id is RequestPriority.LOW.name().
    // For medium priority requests, the RequestQueue id is detector id. The objective
    // is to separate requests from different detectors and fairly process requests
    // from each detector.
    protected final ConcurrentSkipListMap<String, RequestQueue> requestQueues;
    private String lastSelectedRequestQueueId;
    protected Random random;
    private ADCircuitBreakerService adCircuitBreakerService;
    protected ThreadPool threadPool;
    protected Instant cooldownStart;
    protected int coolDownMinutes;
    private float maxQueuedTaskRatio;
    protected Clock clock;
    private float mediumRequestQueuePruneRatio;
    private float lowRequestQueuePruneRatio;
    protected int maintenanceFreqConstant;
    private final Duration stateTtl;
    protected final NodeStateManager nodeStateManager;

    public RateLimitedRequestWorker(
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
        float mediumRequestQueuePruneRatio,
        float lowRequestQueuePruneRatio,
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
        this.mediumRequestQueuePruneRatio = mediumRequestQueuePruneRatio;
        this.lowRequestQueuePruneRatio = lowRequestQueuePruneRatio;

        this.lastSelectedRequestQueueId = null;
        this.requestQueues = new ConcurrentSkipListMap<>();
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
        if (true == requestQueues.isEmpty()) {
            return Optional.empty();
        }

        String startId = lastSelectedRequestQueueId;
        try {
            for (int i = 0; i < requestQueues.size(); i++) {
                if (startId == null || requestQueues.size() == 1 || startId.equals(requestQueues.lastKey())) {
                    startId = requestQueues.firstKey();
                } else {
                    startId = requestQueues.higherKey(startId);
                }

                if (startId.equals(RequestPriority.LOW.name())) {
                    continue;
                }

                RequestQueue requestQueue = requestQueues.get(startId);
                if (requestQueue == null) {
                    continue;
                }

                BlockingQueue<RequestType> requests = requestQueue.content;

                if (requests != null && false == requests.isEmpty()) {
                    clearExpiredRequests(requests);

                    if (false == requests.isEmpty()) {
                        return Optional.of(requests);
                    }
                }
            }

            RequestQueue requestQueue = requestQueues.get(RequestPriority.LOW.name());

            if (requestQueue != null) {
                BlockingQueue<RequestType> requests = requestQueue.content;
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
            // be precise. The objective is to select each RequestQueue with equal probability.
            lastSelectedRequestQueueId = startId;
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
            // consider MEDIUM priority here because only medium priority RequestQueues use
            // detector id as the key of the RequestQueue map. low and high priority requests
            // just use the RequestQueue priority (i.e., low or high) as the key of the RequestQueue map.
            RequestQueue requestQueue = requestQueues
                .computeIfAbsent(
                    RequestPriority.MEDIUM == request.getPriority() ? request.getDetectorId() : request.getPriority().name(),
                    k -> new RequestQueue()
                );

            requestQueue.lastAccessTime = clock.instant();
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

    private void prune(Map<String, RequestQueue> requestQueues) {
        for (Map.Entry<String, RequestQueue> requestQueueEntry : requestQueues.entrySet()) {
            if (requestQueueEntry.getKey().equals(RequestPriority.HIGH.name())) {
                continue;
            }
            // remove more requests in the low priority RequestQueue
            float removeRatio = mediumRequestQueuePruneRatio;
            if (requestQueueEntry.getKey().equals(RequestPriority.LOW.name())) {
                removeRatio = lowRequestQueuePruneRatio;
            }

            RequestQueue requestQueue = requestQueueEntry.getValue();

            if (requestQueue == null) {
                continue;
            }

            BlockingQueue<RequestType> requestQueueContent = requestQueue.content;
            // remove 10% of old requests
            int deletedRequests = (int) (requestQueueContent.size() * removeRatio);
            while (requestQueueContent != null && false == requestQueueContent.isEmpty() && deletedRequests-- >= 0) {
                requestQueueContent.poll();
            }
        }
    }

    private void maintainForMemory() {
        // removed expired RequestQueue
        maintenance(requestQueues, stateTtl);

        if (isSizeExceeded()) {
            // remove until reaching below queueSize
            do {
                prune(requestQueues);
            } while (isSizeExceeded());
        } else if (adCircuitBreakerService.isOpen()) {
            // remove a few items in each RequestQueue
            prune(requestQueues);
        }
    }

    private boolean isSizeExceeded() {
        Collection<RequestQueue> queues = requestQueues.values();
        int totalSize = 0;

        // When faced with a backlog beyond the limit, we prefer fresh requests
        // and throws away old requests.
        // release space so that put won't block
        for (RequestQueue q : queues) {
            totalSize += q.size();
        }
        return totalSize >= queueSize;
    }

    @Override
    public void maintenance() {
        try {
            maintainForMemory();
            maintainForThreadPool();
        } catch (Exception e) {
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
