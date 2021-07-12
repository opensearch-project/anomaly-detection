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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_BATCH_SIZE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_SECS;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
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
public class ColdEntityWorker extends RateLimitedRequestWorker<EntityFeatureRequest> {
    private static final Logger LOG = LogManager.getLogger(ColdEntityWorker.class);
    public static final String WORKER_NAME = "cold-entity";

    private volatile int batchSize;
    private final CheckpointReadWorker checkpointReadQueue;
    // indicate whether a future pull over cold entity queues is scheduled
    private boolean scheduled;
    private volatile int expectedExecutionTimeInSecsPerRequest;

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
            stateTtl,
            nodeStateManager
        );

        this.batchSize = CHECKPOINT_READ_QUEUE_BATCH_SIZE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CHECKPOINT_READ_QUEUE_BATCH_SIZE, it -> this.batchSize = it);

        this.checkpointReadQueue = checkpointReadQueue;
        this.scheduled = false;

        this.expectedExecutionTimeInSecsPerRequest = AnomalyDetectorSettings.EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_SECS.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_SECS, it -> this.expectedExecutionTimeInSecsPerRequest = it);
    }

    private void pullRequests() {
        int pulledRequestSize = 0;
        int filteredRequestSize = 0;
        try {
            List<EntityFeatureRequest> requests = getRequests(batchSize);
            if (requests == null || requests.isEmpty()) {
                return;
            }
            // pulledRequestSize > batchSize means there are more requests in the queue
            pulledRequestSize = requests.size();
            // guarantee we only send low priority requests
            List<EntityFeatureRequest> filteredRequests = requests
                .stream()
                .filter(request -> request.priority == RequestPriority.LOW)
                .collect(Collectors.toList());
            if (!filteredRequests.isEmpty()) {
                checkpointReadQueue.putAll(filteredRequests);
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
            threadPool.schedule(this::pullRequests, delay, AnomalyDetectorPlugin.AD_THREAD_POOL_NAME);
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
     * system more time to processing requests.  We ddd randomness to cope with the
     * case that we want to execute at least 1 request every few seconds, but
     * cannot guarantee that.
     * @param requestSize requests to process
     * @return the delay for the next scheduled run
     */
    private TimeValue getScheduleDelay(int requestSize) {
        int expectedSingleRequestExecutionMillis = 1000 * expectedExecutionTimeInSecsPerRequest;
        int waitMilliSeconds = requestSize * expectedSingleRequestExecutionMillis;
        return TimeValue.timeValueMillis(waitMilliSeconds + random.nextInt(waitMilliSeconds));
    }
}
