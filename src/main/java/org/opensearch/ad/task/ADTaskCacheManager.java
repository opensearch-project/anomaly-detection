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

package org.opensearch.ad.task;

import static org.opensearch.ad.MemoryTracker.Origin.HISTORICAL_SINGLE_ENTITY_DETECTOR;
import static org.opensearch.ad.constant.CommonErrorMessages.DETECTOR_IS_RUNNING;
import static org.opensearch.ad.constant.CommonErrorMessages.EXCEED_HISTORICAL_ANALYSIS_LIMIT;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_CACHED_DELETED_TASKS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.NUM_TREES;
import static org.opensearch.ad.util.ParseUtils.isNullOrEmpty;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.MemoryTracker;
import org.opensearch.ad.common.exception.DuplicateTaskException;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.google.common.collect.ImmutableList;

public class ADTaskCacheManager {
    private final Logger logger = LogManager.getLogger(ADTaskCacheManager.class);

    private volatile Integer maxAdBatchTaskPerNode;
    private volatile Integer maxCachedDeletedTask;
    private final MemoryTracker memoryTracker;
    private final int numberSize = 8;
    public static final int TASK_RETRY_LIMIT = 3;
    private final Semaphore cleanExpiredHCBatchTaskRunStatesSemaphore;

    // ===================================================================
    // Fields below are caches on coordinating node
    // ===================================================================
    /**
     * This field is to cache all detector level tasks which running on the
     * coordinating node to resolve race condition. Will check if detector id
     * exists in cache or not first. If user starts multiple tasks for the same
     * detector, we will put the first task in cache and reject following tasks.
     * <p>Node: coordinating node</p>
     * <p>Key: detector id; Value: detector level task id</p>
     */
    private Map<String, String> detectorTasks;
    /**
     * This field is to cache all HC detector level data on coordinating node, like
     * pending/running entities, check more details in comments of {@link ADHCBatchTaskCache}.
     * <p>Node: coordinating node</p>
     * <p>Key: detector id</p>
     */
    private Map<String, ADHCBatchTaskCache> hcBatchTaskCaches;
    /**
     * This field is to cache all detectors' task slot and task lane limit on coordinating
     * node.
     * <p>Node: coordinating node</p>
     * <p>Key: detector id</p>
     */
    private Map<String, ADTaskSlotLimit> detectorTaskSlotLimit;
    /**
     * This field is to cache all realtime tasks on coordinating node.
     * <p>Node: coordinating node</p>
     * <p>Key is detector id</p>
     */
    private Map<String, ADRealtimeTaskCache> realtimeTaskCaches;
    /**
     * This field is to cache all deleted detector level tasks on coordinating node.
     * Will try to clean up child task and AD result later.
     * <p>Node: coordinating node</p>
     * Check {@link ADTaskManager#cleanChildTasksAndADResultsOfDeletedTask()}
     */
    private Queue<String> deletedDetectorTasks;

    // ===================================================================
    // Fields below are caches on worker node
    // ===================================================================
    /**
     * This field is to cache all batch tasks running on worker node. Both single
     * entity detector task and HC entity task will be cached in this field.
     * <p>Node: worker node</p>
     * <p>Key: task id</p>
     */
    private final Map<String, ADBatchTaskCache> batchTaskCaches;

    // ===================================================================
    // Fields below are caches on both coordinating and worker node
    // ===================================================================
    /**
     * This field is to cache HC detector batch task running state on worker node.
     * For example, is detector historical analysis cancelled or not, HC detector
     * level task state.
     * <p>Node: worker node</p>
     * <p>Outer Key: detector Id; Inner Key: detector level task id</p>
     */
    private Map<String, Map<String, ADHCBatchTaskRunState>> hcBatchTaskRunState;

    // ===================================================================
    // Fields below are caches on any data node serves delete detector
    // request. Check ADTaskManager#deleteADResultOfDetector
    // ===================================================================
    /**
     * This field is to cache deleted detector IDs. Hourly cron will poll this queue
     * and clean AD results. Check {@link ADTaskManager#cleanADResultOfDeletedDetector()}
     * <p>Node: any data node servers delete detector request</p>
     */
    private Queue<String> deletedDetectors;

    /**
     * Constructor to create AD task cache manager.
     *
     * @param settings ES settings
     * @param clusterService ES cluster service
     * @param memoryTracker AD memory tracker
     */
    public ADTaskCacheManager(Settings settings, ClusterService clusterService, MemoryTracker memoryTracker) {
        this.maxAdBatchTaskPerNode = MAX_BATCH_TASK_PER_NODE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_BATCH_TASK_PER_NODE, it -> maxAdBatchTaskPerNode = it);
        this.maxCachedDeletedTask = MAX_CACHED_DELETED_TASKS.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_CACHED_DELETED_TASKS, it -> maxCachedDeletedTask = it);
        this.batchTaskCaches = new ConcurrentHashMap<>();
        this.memoryTracker = memoryTracker;
        this.detectorTasks = new ConcurrentHashMap<>();
        this.hcBatchTaskCaches = new ConcurrentHashMap<>();
        this.realtimeTaskCaches = new ConcurrentHashMap<>();
        this.deletedDetectorTasks = new ConcurrentLinkedQueue<>();
        this.deletedDetectors = new ConcurrentLinkedQueue<>();
        this.detectorTaskSlotLimit = new ConcurrentHashMap<>();
        this.hcBatchTaskRunState = new ConcurrentHashMap<>();
        this.cleanExpiredHCBatchTaskRunStatesSemaphore = new Semaphore(1);
    }

    /**
     * Put AD task into cache.
     * If AD task is already in cache, will throw {@link IllegalArgumentException}
     * If there is one AD task in cache for detector, will throw {@link IllegalArgumentException}
     * If there is not enough memory for this AD task, will throw {@link LimitExceededException}
     *
     * @param adTask AD task
     */
    public synchronized void add(ADTask adTask) {
        String taskId = adTask.getTaskId();
        String detectorId = adTask.getDetectorId();
        if (contains(taskId)) {
            throw new DuplicateTaskException(DETECTOR_IS_RUNNING);
        }
        // It's possible that multiple entity tasks of one detector run on same data node.
        if (!adTask.isEntityTask() && containsTaskOfDetector(detectorId)) {
            throw new DuplicateTaskException(DETECTOR_IS_RUNNING);
        }
        checkRunningTaskLimit();
        long neededCacheSize = calculateADTaskCacheSize(adTask);
        if (!memoryTracker.canAllocateReserved(neededCacheSize)) {
            throw new LimitExceededException("Not enough memory to run detector");
        }
        memoryTracker.consumeMemory(neededCacheSize, true, HISTORICAL_SINGLE_ENTITY_DETECTOR);
        ADBatchTaskCache taskCache = new ADBatchTaskCache(adTask);
        taskCache.getCacheMemorySize().set(neededCacheSize);
        batchTaskCaches.put(taskId, taskCache);
        if (adTask.isEntityTask()) {
            ADHCBatchTaskRunState hcBatchTaskRunState = getHCBatchTaskRunState(detectorId, adTask.getDetectorLevelTaskId());
            if (hcBatchTaskRunState != null) {
                hcBatchTaskRunState.setLastTaskRunTimeInMillis(Instant.now().toEpochMilli());
            }
        }
        // clean expired HC batch task run states when new task starts on worker node.
        cleanExpiredHCBatchTaskRunStates();
    }

    /**
     * Put detector id in running detector cache.
     *
     * @param detectorId detector id
     * @param adTask AD task
     * @throws DuplicateTaskException throw DuplicateTaskException when the detector id already in cache
     */
    public synchronized void add(String detectorId, ADTask adTask) {
        if (detectorTasks.containsKey(detectorId)) {
            logger.warn("detector is already in running detector cache, detectorId: " + detectorId);
            throw new DuplicateTaskException(DETECTOR_IS_RUNNING);
        }
        logger.info("add detector in running detector cache, detectorId: {}, taskId: {}", detectorId, adTask.getTaskId());
        this.detectorTasks.put(detectorId, adTask.getTaskId());
        if (ADTaskType.HISTORICAL_HC_DETECTOR.name().equals(adTask.getTaskType())) {
            ADHCBatchTaskCache adhcBatchTaskCache = new ADHCBatchTaskCache();
            this.hcBatchTaskCaches.put(detectorId, adhcBatchTaskCache);
        }
        // If new historical analysis starts, clean its old batch task run state directly.
        hcBatchTaskRunState.remove(detectorId);
    }

    /**
     * check if current running batch task on current node exceeds
     * max running task limitation.
     * If executing task count exceeds limitation, will throw
     * {@link LimitExceededException}
     */
    public void checkRunningTaskLimit() {
        if (size() >= maxAdBatchTaskPerNode) {
            String error = EXCEED_HISTORICAL_ANALYSIS_LIMIT + ": " + maxAdBatchTaskPerNode;
            throw new LimitExceededException(error);
        }
    }

    /**
     * Get task RCF model.
     * If task doesn't exist in cache, will throw {@link java.lang.IllegalArgumentException}.
     *
     * @param taskId AD task id
     * @return RCF model
     */
    public ThresholdedRandomCutForest getTRcfModel(String taskId) {
        return getBatchTaskCache(taskId).getTRcfModel();
    }

    /**
     * Get threshhold model training data size in bytes.
     *
     * @param taskId task id
     * @return training data size in bytes
     */
    public int getThresholdModelTrainingDataSize(String taskId) {
        return getBatchTaskCache(taskId).getThresholdModelTrainingDataSize().get();
    }

    /**
     * Threshold model trained or not.
     * If task doesn't exist in cache, will throw {@link java.lang.IllegalArgumentException}.
     *
     * @param taskId AD task id
     * @return true if threshold model trained; otherwise, return false
     */
    public boolean isThresholdModelTrained(String taskId) {
        return getBatchTaskCache(taskId).isThresholdModelTrained();
    }

    /**
     * Set threshold model trained or not.
     *
     * @param taskId task id
     * @param trained threshold model trained or not
     */
    protected void setThresholdModelTrained(String taskId, boolean trained) {
        ADBatchTaskCache taskCache = getBatchTaskCache(taskId);
        taskCache.setThresholdModelTrained(trained);
    }

    /**
     * Get shingle data.
     *
     * @param taskId AD task id
     * @return shingle data
     */
    public Deque<Map.Entry<Long, Optional<double[]>>> getShingle(String taskId) {
        return getBatchTaskCache(taskId).getShingle();
    }

    /**
     * Check if task exists in cache.
     *
     * @param taskId task id
     * @return true if task exists in cache; otherwise, return false.
     */
    public boolean contains(String taskId) {
        return batchTaskCaches.containsKey(taskId);
    }

    /**
     * Check if there is task in cache for detector.
     *
     * @param detectorId detector id
     * @return true if there is task in cache; otherwise return false
     */
    public boolean containsTaskOfDetector(String detectorId) {
        return batchTaskCaches.values().stream().filter(v -> Objects.equals(detectorId, v.getDetectorId())).findAny().isPresent();
    }

    /**
     * Get task id list of detector.
     *
     * @param detectorId detector id
     * @return list of task id
     */
    public List<String> getTasksOfDetector(String detectorId) {
        return batchTaskCaches
            .values()
            .stream()
            .filter(v -> Objects.equals(detectorId, v.getDetectorId()))
            .map(c -> c.getTaskId())
            .collect(Collectors.toList());
    }

    /**
     * Get batch task cache. If task doesn't exist in cache, will throw
     * {@link java.lang.IllegalArgumentException}
     * We throw exception rather than return {@code Optional.empty} or null
     * here, so don't need to check task existence by writing duplicate null
     * checking code. All AD task exceptions will be handled in AD task manager.
     *
     * @param taskId task id
     * @return AD batch task cache
     */
    private ADBatchTaskCache getBatchTaskCache(String taskId) {
        if (!contains(taskId)) {
            throw new IllegalArgumentException("AD task not in cache");
        }
        return batchTaskCaches.get(taskId);
    }

    private List<ADBatchTaskCache> getBatchTaskCacheByDetectorId(String detectorId) {
        return batchTaskCaches.values().stream().filter(v -> Objects.equals(detectorId, v.getDetectorId())).collect(Collectors.toList());
    }

    /**
     * Calculate AD task cache memory usage.
     *
     * @param adTask AD task
     * @return how many bytes will consume
     */
    private long calculateADTaskCacheSize(ADTask adTask) {
        AnomalyDetector detector = adTask.getDetector();
        int dimension = detector.getEnabledFeatureIds().size() * detector.getShingleSize();
        return memoryTracker
            .estimateTRCFModelSize(
                dimension,
                NUM_TREES,
                AnomalyDetectorSettings.BATCH_BOUNDING_BOX_CACHE_RATIO,
                detector.getShingleSize().intValue(),
                false
            ) + shingleMemorySize(detector.getShingleSize(), detector.getEnabledFeatureIds().size());
    }

    /**
     * Get RCF model size in bytes.
     *
     * @param taskId task id
     * @return model size in bytes
     */
    public long getModelSize(String taskId) {
        ADBatchTaskCache batchTaskCache = getBatchTaskCache(taskId);
        ThresholdedRandomCutForest tRCF = batchTaskCache.getTRcfModel();
        RandomCutForest rcfForest = tRCF.getForest();
        int dimensions = rcfForest.getDimensions();
        int numberOfTrees = rcfForest.getNumberOfTrees();
        return memoryTracker
            .estimateTRCFModelSize(dimensions, numberOfTrees, AnomalyDetectorSettings.BATCH_BOUNDING_BOX_CACHE_RATIO, 1, false);
    }

    /**
     * Remove task from cache and refresh last run time of HC batch task run state.
     * Don't remove all detector cache here as it's possible that some entity task running on other worker nodes
     *
     * @param taskId AD task id
     * @param detectorId detector id
     * @param detectorTaskId detector level task id
     */
    public void remove(String taskId, String detectorId, String detectorTaskId) {
        ADBatchTaskCache taskCache = batchTaskCaches.get(taskId);
        if (taskCache != null) {
            logger.debug("Remove batch task from cache, task id: {}", taskId);
            memoryTracker.releaseMemory(taskCache.getCacheMemorySize().get(), true, HISTORICAL_SINGLE_ENTITY_DETECTOR);
            batchTaskCaches.remove(taskId);
            ADHCBatchTaskRunState hcBatchTaskRunState = getHCBatchTaskRunState(detectorId, detectorTaskId);
            if (hcBatchTaskRunState != null) {
                hcBatchTaskRunState.setLastTaskRunTimeInMillis(Instant.now().toEpochMilli());
            }
        }
    }

    /**
     * Only remove detector cache if no running entities.
     *
     * @param detectorId detector id
     */
    public void removeHistoricalTaskCacheIfNoRunningEntity(String detectorId) {
        ADHCBatchTaskCache taskCache = hcBatchTaskCaches.get(detectorId);
        if (taskCache != null) {
            if (taskCache.hasRunningEntity()) {
                throw new IllegalArgumentException("HC detector still has running entities");
            }
        }
        removeHistoricalTaskCache(detectorId);
    }

    /**
     * Remove detector id from running detector cache
     *
     * @param detectorId detector id
     */
    public void removeHistoricalTaskCache(String detectorId) {
        ADHCBatchTaskCache taskCache = hcBatchTaskCaches.get(detectorId);
        if (taskCache != null) {
            // this will happen only on coordinating node. When worker nodes left,
            // we will reset task state as STOPPED and clean up cache, add this warning
            // to make it easier to debug issue.
            if (hasEntity(detectorId)) {
                logger
                    .warn(
                        "There are still entities for detector. pending: {}, running: {}, temp: {}",
                        Arrays.toString(taskCache.getPendingEntities()),
                        Arrays.toString(taskCache.getRunningEntities()),
                        Arrays.toString(taskCache.getTempEntities())
                    );
            }
            taskCache.clear();
            hcBatchTaskCaches.remove(detectorId);
        }
        List<String> tasksOfDetector = getTasksOfDetector(detectorId);
        for (String taskId : tasksOfDetector) {
            remove(taskId, null, null);
        }
        if (tasksOfDetector.size() > 0) {
            logger
                .warn(
                    "Removed historical AD task from cache for detector {}, taskId: {}",
                    detectorId,
                    Arrays.toString(tasksOfDetector.toArray(new String[0]))
                );
        }
        if (detectorTasks.containsKey(detectorId)) {
            detectorTasks.remove(detectorId);
            logger.info("Removed detector from AD task coordinating node cache, detectorId: " + detectorId);
        }
        detectorTaskSlotLimit.remove(detectorId);
        hcBatchTaskRunState.remove(detectorId);
    }

    /**
     * Cancel AD task by detector id.
     *
     * @param detectorId detector id
     * @param detectorTaskId detector level task id
     * @param reason why need to cancel task
     * @param userName user name
     * @return AD task cancellation state
     */
    public ADTaskCancellationState cancelByDetectorId(String detectorId, String detectorTaskId, String reason, String userName) {
        if (detectorId == null || detectorTaskId == null) {
            throw new IllegalArgumentException("Can't cancel task for null detector id or detector task id");
        }
        ADHCBatchTaskCache hcTaskCache = hcBatchTaskCaches.get(detectorId);
        List<ADBatchTaskCache> taskCaches = getBatchTaskCacheByDetectorId(detectorId);
        if (hcTaskCache != null) {
            // coordinating node
            logger.debug("Set HC historical analysis as cancelled for detector {}", detectorId);
            hcTaskCache.clearPendingEntities();
            hcTaskCache.setEntityTaskLanes(0);
        }
        ADHCBatchTaskRunState taskStateCache = getOrCreateHCDetectorTaskStateCache(detectorId, detectorTaskId);
        taskStateCache.setCancelledTimeInMillis(Instant.now().toEpochMilli());
        taskStateCache.setHistoricalAnalysisCancelled(true);
        taskStateCache.setCancelReason(reason);
        taskStateCache.setCancelledBy(userName);

        if (isNullOrEmpty(taskCaches)) {
            return ADTaskCancellationState.NOT_FOUND;
        }

        ADTaskCancellationState cancellationState = ADTaskCancellationState.ALREADY_CANCELLED;
        for (ADBatchTaskCache cache : taskCaches) {
            if (!cache.isCancelled()) {
                cancellationState = ADTaskCancellationState.CANCELLED;
                cache.cancel(reason, userName);
            }
        }
        return cancellationState;
    }

    /**
     * Check if single entity detector level task or HC entity task is cancelled or not.
     *
     * @param taskId AD task id, should not be HC detector level task
     * @return true if task is cancelled; otherwise return false
     */
    public boolean isCancelled(String taskId) {
        // For HC detector, ADBatchTaskCache is entity task.
        ADBatchTaskCache taskCache = getBatchTaskCache(taskId);
        String detectorId = taskCache.getDetectorId();
        String detectorTaskId = taskCache.getDetectorTaskId();

        ADHCBatchTaskRunState taskStateCache = getHCBatchTaskRunState(detectorId, detectorTaskId);
        boolean hcDetectorStopped = false;
        if (taskStateCache != null) {
            hcDetectorStopped = taskStateCache.getHistoricalAnalysisCancelled();
        }
        // If a new entity task comes after cancel event, then we have no chance to set it as cancelled.
        // So we need to check hcDetectorStopped for HC detector to know if it's cancelled or not.
        // For single entity detector, it has just 1 task, just need to check taskCache.isCancelled.
        return taskCache.isCancelled() || hcDetectorStopped;
    }

    /**
     * Get current task count in cache.
     *
     * @return task count
     */
    public int size() {
        return batchTaskCaches.size();
    }

    /**
     * Clear all tasks.
     */
    public void clear() {
        batchTaskCaches.clear();
        detectorTasks.clear();
    }

    /**
     * Estimate max memory usage of model training data.
     * The training data is double and will cache in double array.
     * One double consumes 8 bytes.
     *
     * @param size training data point count
     * @return how many bytes will consume
     */
    public long trainingDataMemorySize(int size) {
        return numberSize * size;
    }

    /**
     * Estimate max memory usage of shingle data.
     * One feature aggregated data point(double) consumes 8 bytes.
     * The shingle data is stored in {@link java.util.Deque}. From testing,
     * other parts except feature data consume 80 bytes.
     *
     * Check {@link ADBatchTaskCache#getShingle()}
     *
     * @param shingleSize shingle data point count
     * @param enabledFeatureSize enabled feature count
     * @return how many bytes will consume
     */
    public long shingleMemorySize(int shingleSize, int enabledFeatureSize) {
        return (80 + numberSize * enabledFeatureSize) * shingleSize;
    }

    /**
     * HC top entity initied or not
     *
     * @param detectorId detector id
     * @return true if top entity inited; otherwise return false
     */
    public synchronized boolean topEntityInited(String detectorId) {
        return hcBatchTaskCaches.containsKey(detectorId) ? hcBatchTaskCaches.get(detectorId).getTopEntitiesInited() : false;
    }

    /**
     * Set top entity inited as true.
     *
     * @param detectorId detector id
     */
    public void setTopEntityInited(String detectorId) {
        getExistingHCTaskCache(detectorId).setTopEntitiesInited(true);
    }

    /**
     * Get pending to run entity count.
     *
     * @param detectorId detector id
     * @return entity count
     */
    public int getPendingEntityCount(String detectorId) {
        return hcBatchTaskCaches.containsKey(detectorId) ? hcBatchTaskCaches.get(detectorId).getPendingEntityCount() : 0;
    }

    /**
     * Get current running entity count in cache of detector.
     *
     * @param detectorId detector id
     * @return count of detector's running entity in cache
     */
    public int getRunningEntityCount(String detectorId) {
        ADHCBatchTaskCache taskCache = hcBatchTaskCaches.get(detectorId);
        if (taskCache != null) {
            return taskCache.getRunningEntityCount();
        }
        return 0;
    }

    public int getTempEntityCount(String detectorId) {
        ADHCBatchTaskCache taskCache = hcBatchTaskCaches.get(detectorId);
        if (taskCache != null) {
            return taskCache.getTempEntityCount();
        }
        return 0;
    }

    /**
     * Get total top entity count for detector.
     *
     * @param detectorId detector id
     * @return total top entity count
     */
    public Integer getTopEntityCount(String detectorId) {
        ADHCBatchTaskCache batchTaskCache = hcBatchTaskCaches.get(detectorId);
        if (batchTaskCache != null) {
            return batchTaskCache.getTopEntityCount();
        } else {
            return 0;
        }
    }

    /**
     * Get current running entities of detector.
     * Profile API will call this method.
     *
     * @param detectorId detector id
     * @return detector's running entities in cache
     */
    public List<String> getRunningEntities(String detectorId) {
        if (hcBatchTaskCaches.containsKey(detectorId)) {
            ADHCBatchTaskCache hcTaskCache = getExistingHCTaskCache(detectorId);
            return Arrays.asList(hcTaskCache.getRunningEntities());
        }
        return null;
    }

    /**
     * Set max allowed running entities for HC detector.
     *
     * @param detectorId detector id
     * @param allowedRunningEntities max allowed running entities
     */
    public void setAllowedRunningEntities(String detectorId, int allowedRunningEntities) {
        logger.debug("Set allowed running entities of detector {} as {}", detectorId, allowedRunningEntities);
        getExistingHCTaskCache(detectorId).setEntityTaskLanes(allowedRunningEntities);
    }

    /**
     * Set detector task slots. We cache task slots assigned to detector on coordinating node.
     * When start new historical analysis, will gather detector task slots on all nodes and
     * check how many task slots available for new historical analysis.
     *
     * @param detectorId detector id
     * @param taskSlots task slots
     */
    public synchronized void setDetectorTaskSlots(String detectorId, int taskSlots) {
        logger.debug("Set task slots of detector {} as {}", detectorId, taskSlots);
        ADTaskSlotLimit adTaskSlotLimit = detectorTaskSlotLimit
            .computeIfAbsent(detectorId, key -> new ADTaskSlotLimit(taskSlots, taskSlots));
        adTaskSlotLimit.setDetectorTaskSlots(taskSlots);
    }

    /**
     * Scale up detector task slots.
     * @param detectorId detector id
     * @param delta scale delta
     */
    public synchronized void scaleUpDetectorTaskSlots(String detectorId, int delta) {
        ADTaskSlotLimit adTaskSlotLimit = detectorTaskSlotLimit.get(detectorId);
        int taskSlots = this.getDetectorTaskSlots(detectorId);
        if (adTaskSlotLimit != null && delta > 0) {
            int newTaskSlots = adTaskSlotLimit.getDetectorTaskSlots() + delta;
            logger.info("Scale up task slots of detector {} from {} to {}", detectorId, taskSlots, newTaskSlots);
            adTaskSlotLimit.setDetectorTaskSlots(newTaskSlots);
        }
    }

    /**
     * Check how many unfinished entities in cache. If it's less than detector task slots, we
     * can scale down detector task slots to same as unfinished entities count. We can save
     * task slots in this way. The released task slots can be reused for other task run.
     * @param detectorId detector id
     * @param delta scale delta
     * @return new task slots
     */
    public synchronized int scaleDownHCDetectorTaskSlots(String detectorId, int delta) {
        ADTaskSlotLimit adTaskSlotLimit = this.detectorTaskSlotLimit.get(detectorId);
        int taskSlots = this.getDetectorTaskSlots(detectorId);
        if (adTaskSlotLimit != null && delta > 0) {
            int newTaskSlots = taskSlots - delta;
            if (newTaskSlots > 0) {
                logger.info("Scale down task slots of detector {} from {} to {}", detectorId, taskSlots, newTaskSlots);
                adTaskSlotLimit.setDetectorTaskSlots(newTaskSlots);
                return newTaskSlots;
            }
        }
        return taskSlots;
    }

    /**
     * Set detector task lane limit.
     * @param detectorId detector id
     * @param taskLaneLimit task lane limit
     */
    public synchronized void setDetectorTaskLaneLimit(String detectorId, int taskLaneLimit) {
        ADTaskSlotLimit adTaskSlotLimit = detectorTaskSlotLimit.get(detectorId);
        if (adTaskSlotLimit != null) {
            adTaskSlotLimit.setDetectorTaskLaneLimit(taskLaneLimit);
        }
    }

    /**
     * Get how many task slots assigned to detector
     * @param detectorId detector id
     * @return detector task slot count
     */
    public int getDetectorTaskSlots(String detectorId) {
        ADTaskSlotLimit taskSlotLimit = detectorTaskSlotLimit.get(detectorId);
        if (taskSlotLimit != null) {
            return taskSlotLimit.getDetectorTaskSlots();
        }
        return 0;
    }

    public int getUnfinishedEntityCount(String detectorId) {
        ADHCBatchTaskCache taskCache = hcBatchTaskCaches.get(detectorId);
        if (taskCache != null) {
            return taskCache.getUnfinishedEntityCount();
        }
        return 0;
    }

    /**
     * Get total task slots on this node.
     * @return total task slots
     */
    public int getTotalDetectorTaskSlots() {
        int totalTaskSLots = 0;
        for (Map.Entry<String, ADTaskSlotLimit> entry : detectorTaskSlotLimit.entrySet()) {
            totalTaskSLots += entry.getValue().getDetectorTaskSlots();
        }
        return totalTaskSLots;
    }

    public int getTotalBatchTaskCount() {
        return batchTaskCaches.size();
    }

    /**
     * Get current allowed entity task lanes and decrease it by 1.
     *
     * @param detectorId detector id
     * @return current allowed entity task lane count
     */
    public synchronized int getAndDecreaseEntityTaskLanes(String detectorId) {
        return getExistingHCTaskCache(detectorId).getAndDecreaseEntityTaskLanes();
    }

    /**
     * Get current available new entity task lanes.
     * @param detectorId detector id
     * @return how many task lane available now
     */
    public int getAvailableNewEntityTaskLanes(String detectorId) {
        return getExistingHCTaskCache(detectorId).getEntityTaskLanes();
    }

    private ADHCBatchTaskCache getExistingHCTaskCache(String detectorId) {
        ADHCBatchTaskCache taskCache = hcBatchTaskCaches.get(detectorId);
        if (taskCache != null) {
            return taskCache;
        } else {
            throw new IllegalArgumentException("Can't find HC detector in cache");
        }
    }

    /**
     * Add list of entities into pending entities queue. And will remove these entities
     * from temp entities queue.
     *
     * @param detectorId detector id
     * @param entities list of entities
     */
    public void addPendingEntities(String detectorId, List<String> entities) {
        getExistingHCTaskCache(detectorId).addPendingEntities(entities);
    }

    /**
     * Check if there is any HC task running on current node.
     * @param detectorId detector id
     * @return true if find detector id in any entity task or HC cache
     */
    public boolean isHCTaskRunning(String detectorId) {
        if (isHCTaskCoordinatingNode(detectorId)) {
            return true;
        }
        // Only running tasks will be in cache.
        Optional<ADBatchTaskCache> entityTask = this.batchTaskCaches
            .values()
            .stream()
            .filter(cache -> Objects.equals(detectorId, cache.getDetectorId()) && cache.getEntity() != null)
            .findFirst();
        return entityTask.isPresent();
    }

    /**
     * Check if current node is coordianting node of HC detector.
     * @param detectorId detector id
     * @return true if find detector id in HC cache
     */
    public boolean isHCTaskCoordinatingNode(String detectorId) {
        return hcBatchTaskCaches.containsKey(detectorId);
    }

    /**
     * Set top entity count.
     *
     * @param detectorId detector id
     * @param count top entity count
     */
    public void setTopEntityCount(String detectorId, Integer count) {
        ADHCBatchTaskCache hcTaskCache = getExistingHCTaskCache(detectorId);
        hcTaskCache.setTopEntityCount(count);
        ADTaskSlotLimit adTaskSlotLimit = detectorTaskSlotLimit.get(detectorId);

        if (count != null && adTaskSlotLimit != null) {
            Integer detectorTaskSlots = adTaskSlotLimit.getDetectorTaskSlots();
            if (detectorTaskSlots != null && detectorTaskSlots > count) {
                logger.debug("Scale down task slots from {} to the same as top entity count {}", detectorTaskSlots, count);
                adTaskSlotLimit.setDetectorTaskSlots(count);
            }
        }
    }

    /**
     * Poll one entity from HC detector entities cache. If entity exists, will move
     * entity to temp entites cache; otherwise return null.
     *
     * @param detectorId detector id
     * @return one entity
     */
    public synchronized String pollEntity(String detectorId) {
        if (this.hcBatchTaskCaches.containsKey(detectorId)) {
            ADHCBatchTaskCache hcTaskCache = this.hcBatchTaskCaches.get(detectorId);
            String entity = hcTaskCache.pollEntity();
            return entity;
        } else {
            return null;
        }
    }

    /**
     * Add entity into pending entities queue. And will remove the entity from temp
     * and running entities queue.
     *
     * @param detectorId detector id
     * @param entity entity value
     */
    public void addPendingEntity(String detectorId, String entity) {
        addPendingEntities(detectorId, ImmutableList.of(entity));
    }

    /**
     * Move one entity to running entity queue.
     *
     * @param detectorId detector id
     * @param entity entity value
     */
    public void moveToRunningEntity(String detectorId, String entity) {
        ADHCBatchTaskCache hcTaskCache = hcBatchTaskCaches.get(detectorId);
        if (hcTaskCache != null) {
            hcTaskCache.moveToRunningEntity(entity);
        }
    }

    /**
     * Task exceeds max retry limit or not.
     *
     * @param detectorId detector id
     * @param taskId task id
     * @return true if exceed retry limit; otherwise return false
     */
    public boolean exceedRetryLimit(String detectorId, String taskId) {
        return getExistingHCTaskCache(detectorId).getTaskRetryTimes(taskId) > TASK_RETRY_LIMIT;
    }

    /**
     * Push entity back to the end of pending entity queue.
     *
     * @param taskId task id
     * @param detectorId detector id
     * @param entity entity value
     */
    public void pushBackEntity(String taskId, String detectorId, String entity) {
        addPendingEntity(detectorId, entity);
        increaseEntityTaskRetry(detectorId, taskId);
    }

    /**
     * Increase entity task retry times.
     *
     * @param detectorId detector id
     * @param taskId task id
     * @return how many times retried
     */
    public int increaseEntityTaskRetry(String detectorId, String taskId) {
        return getExistingHCTaskCache(detectorId).increaseTaskRetry(taskId);
    }

    /**
     * Remove entity from cache.
     *
     * @param detectorId detector id
     * @param entity entity value
     */
    public void removeEntity(String detectorId, String entity) {
        if (hcBatchTaskCaches.containsKey(detectorId)) {
            hcBatchTaskCaches.get(detectorId).removeEntity(entity);
        }
    }

    /**
     * Return AD task's entity list.
     *
     * @param taskId AD task id
     * @return entity
     */
    public Entity getEntity(String taskId) {
        return getBatchTaskCache(taskId).getEntity();
    }

    /**
     * Check if detector still has entity in cache.
     *
     * @param detectorId detector id
     * @return true if detector still has entity in cache
     */
    public synchronized boolean hasEntity(String detectorId) {
        return hcBatchTaskCaches.containsKey(detectorId) && hcBatchTaskCaches.get(detectorId).hasEntity();
    }

    /**
     * Remove entity from HC task running entity cache.
     *
     * @param detectorId detector id
     * @param entity entity
     * @return true if entity was removed as a result of this call
     */
    public boolean removeRunningEntity(String detectorId, String entity) {
        ADHCBatchTaskCache hcTaskCache = hcBatchTaskCaches.get(detectorId);
        if (hcTaskCache != null) {
            boolean removed = hcTaskCache.removeRunningEntity(entity);
            logger.debug("Remove entity from running entities cache: {}: {}", entity, removed);
            return removed;
        }
        return false;
    }

    /**
     * Try to get semaphore to update detector task.
     *
     * If the timeout is less than or equal to zero, will not wait at all to get 1 permit.
     * If permit is available, will acquire 1 permit and return true immediately. If no permit,
     * will wait for other thread release. If no permit available until timeout elapses, will
     * return false.
     *
     * @param detectorId detector id
     * @param timeoutInMillis timeout in milliseconds to wait for a permit, zero or negative means don't wait at all
     * @return true if can get semaphore
     * @throws InterruptedException if the current thread is interrupted
     */
    public boolean tryAcquireTaskUpdatingSemaphore(String detectorId, long timeoutInMillis) throws InterruptedException {
        ADHCBatchTaskCache taskCache = hcBatchTaskCaches.get(detectorId);
        if (taskCache != null) {
            return taskCache.tryAcquireTaskUpdatingSemaphore(timeoutInMillis);
        }
        return false;
    }

    /**
     * Try to release semaphore of updating detector task.
     * @param detectorId detector id
     */
    public void releaseTaskUpdatingSemaphore(String detectorId) {
        ADHCBatchTaskCache taskCache = hcBatchTaskCaches.get(detectorId);
        if (taskCache != null) {
            taskCache.releaseTaskUpdatingSemaphore();
        }
    }

    /**
     * Clear pending entities of HC detector.
     *
     * @param detectorId detector id
     */
    public void clearPendingEntities(String detectorId) {
        ADHCBatchTaskCache taskCache = hcBatchTaskCaches.get(detectorId);
        if (taskCache != null) {
            taskCache.clearPendingEntities();
        }
    }

    /**
     * Check if realtime task field value change needed or not by comparing with cache.
     * 1. If new field value is null, will consider changed needed to this field.
     * 2. will consider the real time task change needed if
     * 1) init progress is larger or the old init progress is null, or
     * 2) if the state is different, and it is not changing from running to init.
     *  for other fields, as long as field values changed, will consider the realtime
     *  task change needed. We did this so that the init progress or state won't go backwards.
     * 3. If realtime task cache not found, will consider the realtime task change needed.
     *
     * @param detectorId detector id
     * @param newState new task state
     * @param newInitProgress new init progress
     * @param newError new error
     * @return true if realtime task change needed.
     */
    public boolean isRealtimeTaskChangeNeeded(String detectorId, String newState, Float newInitProgress, String newError) {
        if (realtimeTaskCaches.containsKey(detectorId)) {
            ADRealtimeTaskCache realtimeTaskCache = realtimeTaskCaches.get(detectorId);
            boolean stateChangeNeeded = false;
            String oldState = realtimeTaskCache.getState();
            if (newState != null
                && !newState.equals(oldState)
                && !(ADTaskState.INIT.name().equals(newState) && ADTaskState.RUNNING.name().equals(oldState))) {
                stateChangeNeeded = true;
            }
            boolean initProgressChangeNeeded = false;
            Float existingProgress = realtimeTaskCache.getInitProgress();
            if (newInitProgress != null
                && !newInitProgress.equals(existingProgress)
                && (existingProgress == null || newInitProgress > existingProgress)) {
                initProgressChangeNeeded = true;
            }
            boolean errorChanged = false;
            if (newError != null && !newError.equals(realtimeTaskCache.getError())) {
                errorChanged = true;
            }
            if (stateChangeNeeded || initProgressChangeNeeded || errorChanged) {
                return true;
            }
            return false;
        } else {
            return true;
        }
    }

    /**
     * Update realtime task cache with new field values. If realtime task cache exist, update it
     * directly if task is not done; if task is done, remove the detector's realtime task cache.
     *
     * If realtime task cache doesn't exist, will do nothing. Next realtime job run will re-init
     * realtime task cache when it finds task cache not inited yet.
     * Check {@link ADTaskManager#initRealtimeTaskCacheAndCleanupStaleCache(String, AnomalyDetector, TransportService, ActionListener)},
     * {@link ADTaskManager#updateLatestRealtimeTaskOnCoordinatingNode(String, String, Long, Long, String, ActionListener)}
     *
     * @param detectorId detector id
     * @param newState new task state
     * @param newInitProgress new init progress
     * @param newError new error
     */
    public void updateRealtimeTaskCache(String detectorId, String newState, Float newInitProgress, String newError) {
        ADRealtimeTaskCache realtimeTaskCache = realtimeTaskCaches.get(detectorId);
        if (realtimeTaskCache != null) {
            if (newState != null) {
                realtimeTaskCache.setState(newState);
            }
            if (newInitProgress != null) {
                realtimeTaskCache.setInitProgress(newInitProgress);
            }
            if (newError != null) {
                realtimeTaskCache.setError(newError);
            }
            if (newState != null && !ADTaskState.NOT_ENDED_STATES.contains(newState)) {
                // If task is done, will remove its realtime task cache.
                logger.info("Realtime task done with state {}, remove RT task cache for detector ", newState, detectorId);
                removeRealtimeTaskCache(detectorId);
            }
        } else {
            logger.debug("Realtime task cache is not inited yet for detector {}", detectorId);
        }
    }

    public void initRealtimeTaskCache(String detectorId, long detectorIntervalInMillis) {
        realtimeTaskCaches.put(detectorId, new ADRealtimeTaskCache(null, null, null, detectorIntervalInMillis));
        logger.debug("Realtime task cache inited");
    }

    public void refreshRealtimeJobRunTime(String detectorId) {
        ADRealtimeTaskCache taskCache = realtimeTaskCaches.get(detectorId);
        if (taskCache != null) {
            taskCache.setLastJobRunTime(Instant.now().toEpochMilli());
        }
    }

    /**
     * Get detector IDs from realtime task cache.
     * @return array of detector id
     */
    public String[] getDetectorIdsInRealtimeTaskCache() {
        return realtimeTaskCaches.keySet().toArray(new String[0]);
    }

    /**
     * Remove detector's realtime task from cache.
     * @param detectorId detector id
     */
    public void removeRealtimeTaskCache(String detectorId) {
        if (realtimeTaskCaches.containsKey(detectorId)) {
            logger.info("Delete realtime cache for detector {}", detectorId);
            realtimeTaskCaches.remove(detectorId);
        }
    }

    public ADRealtimeTaskCache getRealtimeTaskCache(String detectorId) {
        return realtimeTaskCaches.get(detectorId);
    }

    /**
     * Clear realtime task cache.
     */
    public void clearRealtimeTaskCache() {
        realtimeTaskCaches.clear();
    }

    /**
     * Add deleted task's id to deleted detector tasks queue.
     * @param taskId task id
     */
    public void addDeletedDetectorTask(String taskId) {
        if (deletedDetectorTasks.size() < maxCachedDeletedTask) {
            deletedDetectorTasks.add(taskId);
        }
    }

    /**
     * Check if deleted task queue has items.
     * @return true if has deleted detector task in cache
     */
    public boolean hasDeletedDetectorTask() {
        return !deletedDetectorTasks.isEmpty();
    }

    /**
     * Poll one deleted detector task.
     * @return task id
     */
    public String pollDeletedDetectorTask() {
        return this.deletedDetectorTasks.poll();
    }

    /**
     * Add deleted detector's id to deleted detector queue.
     * @param detectorId detector id
     */
    public void addDeletedDetector(String detectorId) {
        if (deletedDetectors.size() < maxCachedDeletedTask) {
            deletedDetectors.add(detectorId);
        }
    }

    /**
     * Poll one deleted detector.
     * @return detector id
     */
    public String pollDeletedDetector() {
        return this.deletedDetectors.poll();
    }

    public String getDetectorTaskId(String detectorId) {
        return detectorTasks.get(detectorId);
    }

    public Instant getLastScaleEntityTaskLaneTime(String detectorId) {
        ADHCBatchTaskCache taskCache = hcBatchTaskCaches.get(detectorId);
        if (taskCache != null) {
            return taskCache.getLastScaleEntityTaskSlotsTime();
        }
        return null;
    }

    public void refreshLastScaleEntityTaskLaneTime(String detectorId) {
        ADHCBatchTaskCache taskCache = hcBatchTaskCaches.get(detectorId);
        if (taskCache != null) {
            taskCache.setLastScaleEntityTaskSlotsTime(Instant.now());
        }
    }

    public Instant getLatestHCTaskRunTime(String detectorId) {
        ADHCBatchTaskCache taskCache = hcBatchTaskCaches.get(detectorId);
        if (taskCache != null) {
            return taskCache.getLatestTaskRunTime();
        }
        return null;
    }

    public void refreshLatestHCTaskRunTime(String detectorId) {
        ADHCBatchTaskCache taskCache = hcBatchTaskCaches.get(detectorId);
        if (taskCache != null) {
            taskCache.refreshLatestTaskRunTime();
        }
    }

    /**
     * Update detector level task's state in cache.
     * @param detectorId detector id
     * @param detectorTaskId detector level task id
     * @param newState new state
     */
    public synchronized void updateDetectorTaskState(String detectorId, String detectorTaskId, String newState) {
        ADHCBatchTaskRunState cache = getOrCreateHCDetectorTaskStateCache(detectorId, detectorTaskId);
        if (cache != null) {
            cache.setDetectorTaskState(newState);
            cache.setLastTaskRunTimeInMillis(Instant.now().toEpochMilli());
        }
    }

    public ADHCBatchTaskRunState getOrCreateHCDetectorTaskStateCache(String detectorId, String detectorTaskId) {
        Map<String, ADHCBatchTaskRunState> states = hcBatchTaskRunState.computeIfAbsent(detectorId, it -> new ConcurrentHashMap<>());
        return states.computeIfAbsent(detectorTaskId, it -> new ADHCBatchTaskRunState());
    }

    public String getDetectorTaskState(String detectorId, String detectorTaskId) {
        ADHCBatchTaskRunState batchTaskRunStates = getHCBatchTaskRunState(detectorId, detectorTaskId);
        if (batchTaskRunStates != null) {
            return batchTaskRunStates.getDetectorTaskState();
        }
        return null;
    }

    public boolean detectorTaskStateExists(String detectorId, String detectorTaskId) {
        Map<String, ADHCBatchTaskRunState> taskStateCache = hcBatchTaskRunState.get(detectorId);
        return taskStateCache != null && taskStateCache.containsKey(detectorTaskId);
    }

    private ADHCBatchTaskRunState getHCBatchTaskRunState(String detectorId, String detectorTaskId) {
        if (detectorId == null || detectorTaskId == null) {
            return null;
        }
        Map<String, ADHCBatchTaskRunState> batchTaskRunStates = hcBatchTaskRunState.get(detectorId);
        if (batchTaskRunStates != null) {
            return batchTaskRunStates.get(detectorTaskId);
        }
        return null;
    }

    /**
     * Check if HC detector's historical analysis cancelled or not.
     *
     * @param detectorId detector id
     * @param detectorTaskId detector level task id
     * @return true if HC detector historical analysis cancelled; otherwise return false
     */
    public boolean isHistoricalAnalysisCancelledForHC(String detectorId, String detectorTaskId) {
        ADHCBatchTaskRunState taskStateCache = getHCBatchTaskRunState(detectorId, detectorTaskId);
        if (taskStateCache != null) {
            return taskStateCache.getHistoricalAnalysisCancelled();
        }
        return false;
    }

    /**
     * Get why task cancelled.
     *
     * @param taskId AD task id
     * @return task cancellation reason
     */
    public String getCancelReason(String taskId) {
        return getBatchTaskCache(taskId).getCancelReason();
    }

    /**
     * Get task cancelled by which user.
     *
     * @param taskId AD task id
     * @return user name
     */
    public String getCancelledBy(String taskId) {
        return getBatchTaskCache(taskId).getCancelledBy();
    }

    public String getCancelledByForHC(String detectorId, String detectorTaskId) {
        ADHCBatchTaskRunState taskCache = getHCBatchTaskRunState(detectorId, detectorTaskId);
        if (taskCache != null) {
            return taskCache.getCancelledBy();
        }
        return null;
    }

    public String getCancelReasonForHC(String detectorId, String detectorTaskId) {
        ADHCBatchTaskRunState taskCache = getHCBatchTaskRunState(detectorId, detectorTaskId);
        if (taskCache != null) {
            return taskCache.getCancelReason();
        }
        return null;
    }

    public void cleanExpiredHCBatchTaskRunStates() {
        if (!cleanExpiredHCBatchTaskRunStatesSemaphore.tryAcquire()) {
            return;
        }
        try {
            List<String> detectorIdOfEmptyStates = new ArrayList<>();
            for (Map.Entry<String, Map<String, ADHCBatchTaskRunState>> detectorRunStates : hcBatchTaskRunState.entrySet()) {
                List<String> taskIdOfExpiredStates = new ArrayList<>();
                String detectorId = detectorRunStates.getKey();
                boolean noRunningTask = isNullOrEmpty(getTasksOfDetector(detectorId));
                Map<String, ADHCBatchTaskRunState> taskRunStates = detectorRunStates.getValue();
                if (taskRunStates == null) {
                    // If detector's task run state is null, add detector id to detectorIdOfEmptyStates and remove it from
                    // hcBatchTaskRunState later.
                    detectorIdOfEmptyStates.add(detectorId);
                    continue;
                }
                if (!noRunningTask) {
                    // If a detector has running task, we should not clean up task run state cache for it.
                    // It's possible that some entity task is on the way to worker node. So we should not
                    // remove detector level state if no running task found. Otherwise the task may arrive
                    // after run state cache deleted, then it can run on work node. We should delete cache
                    // if no running task and run state expired.
                    continue;
                }
                for (Map.Entry<String, ADHCBatchTaskRunState> taskRunState : taskRunStates.entrySet()) {
                    ADHCBatchTaskRunState state = taskRunState.getValue();
                    if (state != null && noRunningTask && state.expired()) {
                        taskIdOfExpiredStates.add(taskRunState.getKey());
                    }
                }
                logger
                    .debug(
                        "Remove expired HC batch task run states for these tasks: {}",
                        Arrays.toString(taskIdOfExpiredStates.toArray(new String[0]))
                    );
                taskIdOfExpiredStates.forEach(id -> taskRunStates.remove(id));
                if (taskRunStates.isEmpty()) {
                    detectorIdOfEmptyStates.add(detectorId);
                }
            }
            logger
                .debug(
                    "Remove empty HC batch task run states for these detectors : {}",
                    Arrays.toString(detectorIdOfEmptyStates.toArray(new String[0]))
                );
            detectorIdOfEmptyStates.forEach(id -> hcBatchTaskRunState.remove(id));
        } catch (Exception e) {
            logger.error("Failed to clean expired HC batch task run states", e);
        } finally {
            cleanExpiredHCBatchTaskRunStatesSemaphore.release();
        }
    }

    /**
     * We query result index to check if there are any result generated for detector to tell whether it passed initialization of not.
     * To avoid repeated query when there is no data, record whether we have done that or not.
     * @param id detector id
     */
    public void markResultIndexQueried(String id) {
        ADRealtimeTaskCache realtimeTaskCache = realtimeTaskCaches.get(id);
        // we initialize a real time cache at the beginning of AnomalyResultTransportAction if it
        // cannot be found. If the cache is empty, we will return early and wait it for it to be
        // initialized.
        if (realtimeTaskCache != null) {
            realtimeTaskCache.setQueriedResultIndex(true);
        }
    }

    /**
     * We query result index to check if there are any result generated for detector to tell whether it passed initialization of not.
     *
     * @param id detector id
     * @return whether we have queried result index or not.
     */
    public boolean hasQueriedResultIndex(String id) {
        ADRealtimeTaskCache realtimeTaskCache = realtimeTaskCaches.get(id);
        if (realtimeTaskCache != null) {
            return realtimeTaskCache.hasQueriedResultIndex();
        }
        return false;
    }
}
