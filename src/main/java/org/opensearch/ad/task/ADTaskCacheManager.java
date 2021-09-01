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

package org.opensearch.ad.task;

import static org.opensearch.ad.MemoryTracker.Origin.HISTORICAL_SINGLE_ENTITY_DETECTOR;
import static org.opensearch.ad.constant.CommonErrorMessages.DETECTOR_IS_RUNNING;
import static org.opensearch.ad.constant.CommonErrorMessages.EXCEED_HISTORICAL_ANALYSIS_LIMIT;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_CACHED_DELETED_TASKS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.NUM_TREES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.THRESHOLD_MODEL_TRAINING_SIZE;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.MemoryTracker;
import org.opensearch.ad.common.exception.DuplicateTaskException;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.ml.ThresholdingModel;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;

import com.amazon.randomcutforest.RandomCutForest;
import com.google.common.collect.ImmutableList;

public class ADTaskCacheManager {
    private final Logger logger = LogManager.getLogger(ADTaskCacheManager.class);
    private final Map<String, ADBatchTaskCache> taskCaches;
    private volatile Integer maxAdBatchTaskPerNode;
    private volatile Integer maxCachedDeletedTask;
    private final MemoryTracker memoryTracker;
    private final int numberSize = 8;
    public static final int TASK_RETRY_LIMIT = 3;

    // We use this field to record all detectors which running on the
    // coordinating node to resolve race condition. We will check if
    // detector id exists in cache or not first. If user starts
    // multiple tasks for the same detector, we will put the first
    // task in cache. For other tasks, we find the detector id exists,
    // that means there is already one task running for this detector,
    // so we will reject the task.
    private Set<String> detectors;

    // Use this field to cache all HC tasks. Key is detector id
    private Map<String, ADHCBatchTaskCache> hcTaskCaches;
    // cache deleted detector level tasks
    private Queue<String> deletedDetectorTasks;

    // This field is to cache all realtime tasks. Key is detector id
    private Map<String, ADRealtimeTaskCache> realtimeTaskCaches;

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
        taskCaches = new ConcurrentHashMap<>();
        this.memoryTracker = memoryTracker;
        this.detectors = Sets.newConcurrentHashSet();
        this.hcTaskCaches = new ConcurrentHashMap<>();
        this.realtimeTaskCaches = new ConcurrentHashMap<>();
        this.deletedDetectorTasks = new ConcurrentLinkedQueue<>();
    }

    /**
     * Put AD task into cache.
     * If AD task is already in cache, will throw {@link IllegalArgumentException}
     * If there is one AD task in cache for detector, will throw {@link IllegalArgumentException}
     * If there is no enough memory for this AD task, will throw {@link LimitExceededException}
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
            throw new LimitExceededException("No enough memory to run detector");
        }
        memoryTracker.consumeMemory(neededCacheSize, true, HISTORICAL_SINGLE_ENTITY_DETECTOR);
        ADBatchTaskCache taskCache = new ADBatchTaskCache(adTask);
        taskCache.getCacheMemorySize().set(neededCacheSize);
        taskCaches.put(taskId, taskCache);
    }

    /**
     * Put detector id in running detector cache.
     *
     * @param detectorId detector id
     * @param taskType task type
     * @throws DuplicateTaskException throw DuplicateTaskException when the detector id already in cache
     */
    public synchronized void add(String detectorId, String taskType) {
        if (detectors.contains(detectorId)) {
            logger.debug("detector is already in running detector cache, detectorId: " + detectorId);
            throw new DuplicateTaskException(DETECTOR_IS_RUNNING);
        }
        logger.debug("add detector in running detector cache, detectorId: " + detectorId);
        this.detectors.add(detectorId);
        if (ADTaskType.HISTORICAL_HC_DETECTOR.name().equals(taskType)) {
            this.hcTaskCaches.put(detectorId, new ADHCBatchTaskCache());
        }
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
    public RandomCutForest getRcfModel(String taskId) {
        return getBatchTaskCache(taskId).getRcfModel();
    }

    /**
     * Get task threshold model.
     * If task doesn't exist in cache, will throw {@link java.lang.IllegalArgumentException}.
     *
     * @param taskId AD task id
     * @return threshold model
     */
    public ThresholdingModel getThresholdModel(String taskId) {
        return getBatchTaskCache(taskId).getThresholdModel();
    }

    /**
     * Get threshold model training data.
     * If task doesn't exist in cache, will throw {@link java.lang.IllegalArgumentException}.
     *
     * @param taskId AD task id
     * @return threshold model training data
     */
    public double[] getThresholdModelTrainingData(String taskId) {
        return getBatchTaskCache(taskId).getThresholdModelTrainingData();
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
     * Add threshold model training data.
     *
     * @param taskId task id
     * @param data training data
     * @return latest threshold model training data size after adding new data
     */
    public int addThresholdModelTrainingData(String taskId, double... data) {
        ADBatchTaskCache taskCache = getBatchTaskCache(taskId);
        double[] thresholdModelTrainingData = taskCache.getThresholdModelTrainingData();
        AtomicInteger size = taskCache.getThresholdModelTrainingDataSize();
        int dataPointsAdded = Math.min(data.length, THRESHOLD_MODEL_TRAINING_SIZE - size.get());
        System.arraycopy(data, 0, thresholdModelTrainingData, size.get(), dataPointsAdded);
        return size.addAndGet(dataPointsAdded);
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
        if (trained) {
            int size = taskCache.getThresholdModelTrainingDataSize().get();
            long cacheSize = trainingDataMemorySize(size);
            taskCache.clearTrainingData();
            taskCache.getCacheMemorySize().getAndAdd(-cacheSize);
            memoryTracker.releaseMemory(cacheSize, true, HISTORICAL_SINGLE_ENTITY_DETECTOR);
        }
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
        return taskCaches.containsKey(taskId);
    }

    /**
     * Check if there is task in cache for detector.
     *
     * @param detectorId detector id
     * @return true if there is task in cache; otherwise return false
     */
    public boolean containsTaskOfDetector(String detectorId) {
        return taskCaches.values().stream().filter(v -> Objects.equals(detectorId, v.getDetectorId())).findAny().isPresent();
    }

    /**
     * Get task id list of detector.
     *
     * @param detectorId detector id
     * @return list of task id
     */
    public List<String> getTasksOfDetector(String detectorId) {
        return taskCaches
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
        return taskCaches.get(taskId);
    }

    private List<ADBatchTaskCache> getBatchTaskCacheByDetectorId(String detectorId) {
        return taskCaches.values().stream().filter(v -> Objects.equals(detectorId, v.getDetectorId())).collect(Collectors.toList());
    }

    /**
     * Calculate AD task cache memory usage.
     *
     * @param adTask AD task
     * @return how many bytes will consume
     */
    private long calculateADTaskCacheSize(ADTask adTask) {
        AnomalyDetector detector = adTask.getDetector();
        return memoryTracker.estimateTotalModelSize(detector, NUM_TREES, AnomalyDetectorSettings.BATCH_BOUNDING_BOX_CACHE_RATIO)
            + trainingDataMemorySize(THRESHOLD_MODEL_TRAINING_SIZE) + shingleMemorySize(
                detector.getShingleSize(),
                detector.getEnabledFeatureIds().size()
            );
    }

    /**
     * Get RCF model size in bytes.
     *
     * @param taskId task id
     * @return model size in bytes
     */
    public long getModelSize(String taskId) {
        ADBatchTaskCache batchTaskCache = getBatchTaskCache(taskId);
        int dimensions = batchTaskCache.getRcfModel().getDimensions();
        int numberOfTrees = batchTaskCache.getRcfModel().getNumberOfTrees();
        return memoryTracker.estimateTotalModelSize(dimensions, numberOfTrees, AnomalyDetectorSettings.BATCH_BOUNDING_BOX_CACHE_RATIO);
    }

    /**
     * Remove task from cache.
     *
     * @param taskId AD task id
     */
    public void remove(String taskId) {
        if (contains(taskId)) {
            ADBatchTaskCache taskCache = getBatchTaskCache(taskId);
            memoryTracker.releaseMemory(taskCache.getCacheMemorySize().get(), true, HISTORICAL_SINGLE_ENTITY_DETECTOR);
            taskCaches.remove(taskId);
            // can't remove detector id from cache here as it's possible that some task running on
            // other worker nodes
        }
    }

    /**
     * Remove detector id from running detector cache
     *
     * @param detectorId detector id
     */
    public void removeDetector(String detectorId) {
        if (detectors.contains(detectorId)) {
            detectors.remove(detectorId);
            logger.debug("Removed detector from AD task coordinating node cache, detectorId: " + detectorId);
        } else {
            logger.debug("Detector is not in AD task coordinating node cache");
        }
    }

    /**
     * Cancel AD task.
     *
     * @param taskId AD task id
     * @param reason why need to cancel task
     * @param userName user name
     * @return AD task cancellation state
     */
    public ADTaskCancellationState cancel(String taskId, String reason, String userName) {
        if (!contains(taskId)) {
            return ADTaskCancellationState.NOT_FOUND;
        }
        if (isCancelled(taskId)) {
            return ADTaskCancellationState.ALREADY_CANCELLED;
        }
        getBatchTaskCache(taskId).cancel(reason, userName);
        return ADTaskCancellationState.CANCELLED;
    }

    /**
     * Cancel AD task by detector id.
     *
     * @param detectorId detector id
     * @param reason why need to cancel task
     * @param userName user name
     * @return AD task cancellation state
     */
    public ADTaskCancellationState cancelByDetectorId(String detectorId, String reason, String userName) {
        List<ADBatchTaskCache> taskCaches = getBatchTaskCacheByDetectorId(detectorId);

        if (taskCaches.isEmpty()) {
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
     * Task is cancelled or not.
     *
     * @param taskId AD task id
     * @return true if task is cancelled; otherwise return false
     */
    public boolean isCancelled(String taskId) {
        ADBatchTaskCache taskCache = getBatchTaskCache(taskId);
        return taskCache.isCancelled();
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

    /**
     * Get current task count in cache.
     *
     * @return task count
     */
    public int size() {
        return taskCaches.size();
    }

    /**
     * Clear all tasks.
     */
    public void clear() {
        taskCaches.clear();
        detectors.clear();
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
        return hcTaskCaches.containsKey(detectorId) ? hcTaskCaches.get(detectorId).getTopEntitiesInited() : false;
    }

    /**
     * Set top entity inited as true.
     *
     * @param detectorId detector id
     */
    public void setTopEntityInited(String detectorId) {
        getHCTaskCache(detectorId).setTopEntitiesInited(true);
    }

    /**
     * Get pending to run entity count.
     *
     * @param detectorId detector id
     * @return entity count
     */
    public int getPendingEntityCount(String detectorId) {
        return hcTaskCaches.containsKey(detectorId) ? hcTaskCaches.get(detectorId).getPendingEntityCount() : 0;
    }

    /**
     * Get current running entity count in cache of detector.
     *
     * @param detectorId detector id
     * @return count of detector's running entity in cache
     */
    public int getRunningEntityCount(String detectorId) {
        return hcTaskCaches.containsKey(detectorId) ? hcTaskCaches.get(detectorId).getRunningEntityCount() : 0;
    }

    /**
     * Get total top entity count for detector.
     *
     * @param detectorId detector id
     * @return total top entity count
     */
    public Integer getTopEntityCount(String detectorId) {
        return hcTaskCaches.containsKey(detectorId) ? hcTaskCaches.get(detectorId).getTopEntityCount() : 0;
    }

    /**
     * Get current running entities of detector.
     * Profile API will call this method.
     *
     * @param detectorId detector id
     * @return detector's running entities in cache
     */
    public String[] getRunningEntities(String detectorId) {
        if (hcTaskCaches.containsKey(detectorId)) {
            ADHCBatchTaskCache hcTaskCache = getExistingHCTaskCache(detectorId);
            return hcTaskCache.getRunningEntities();
        } else {
            return new String[] {};
        }
    }

    /**
     * Set max allowed running entities for HC detector.
     *
     * @param detectorId detector id
     * @param allowedRunningEntities max allowed running entities
     */
    public void setAllowedRunningEntities(String detectorId, int allowedRunningEntities) {
        getExistingHCTaskCache(detectorId).setEntityTaskLanes(allowedRunningEntities);
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

    private ADHCBatchTaskCache getExistingHCTaskCache(String detectorId) {
        if (hcTaskCaches.containsKey(detectorId)) {
            return hcTaskCaches.get(detectorId);
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
        getHCTaskCache(detectorId).addEntities(entities);
    }

    private ADHCBatchTaskCache getHCTaskCache(String detectorId) {
        return hcTaskCaches.computeIfAbsent(detectorId, id -> new ADHCBatchTaskCache());
    }

    /**
     * Set top entity count.
     *
     * @param detectorId detector id
     * @param count top entity count
     */
    public void setTopEntityCount(String detectorId, Integer count) {
        ADHCBatchTaskCache hcTaskCache = getHCTaskCache(detectorId);
        hcTaskCache.setTopEntityCount(count);
    }

    /**
     * Poll one entity from HC detector entities cache. If entity exists, will move
     * entity to temp entites cache; otherwise return null.
     *
     * @param detectorId detector id
     * @return one entity
     */
    public synchronized String pollEntity(String detectorId) {
        if (this.hcTaskCaches.containsKey(detectorId)) {
            ADHCBatchTaskCache hcTaskCache = this.hcTaskCaches.get(detectorId);
            String entity = hcTaskCache.pollEntity();
            return entity;
        } else {
            return null;
        }
    }

    /**
     * Add entity into pending entities queue. And will remove the entity from temp
     * entities queue.
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
    public synchronized void moveToRunningEntity(String detectorId, String entity) {
        if (this.hcTaskCaches.containsKey(detectorId)) {
            ADHCBatchTaskCache hcTaskCache = this.hcTaskCaches.get(detectorId);
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
        if (hcTaskCaches.containsKey(detectorId)) {
            hcTaskCaches.get(detectorId).removeEntity(entity);
        }
    }

    /**
     * Return AD task's entity list.
     * TODO: Currently we only support one category field. Need to support multi-category fields.
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
    public boolean hasEntity(String detectorId) {
        return hcTaskCaches.containsKey(detectorId) && hcTaskCaches.get(detectorId).hasEntity();
    }

    /**
     * Remove entity from HC task running entity cache.
     *
     * @param detectorId detector id
     * @param entity entity
     * @return true if entity was removed as a result of this call
     */
    public boolean removeRunningEntity(String detectorId, String entity) {
        logger.debug("Remove entity from running entities cache: {}", entity);
        if (hcTaskCaches.containsKey(detectorId)) {
            ADHCBatchTaskCache hcTaskCache = hcTaskCaches.get(detectorId);
            return hcTaskCache.removeRunningEntity(entity);
        }
        return false;
    }

    /**
     * Updating detector level task or not.
     * This is to solve version conflict while multiple entity task done messages
     * triggers updating detector level task.
     * @param detectorId detector id
     * @return true if is updating detector task
     */
    public Boolean isDetectorTaskUpdating(String detectorId) {
        if (hcTaskCaches.containsKey(detectorId)) {
            return getExistingHCTaskCache(detectorId).getDetectorTaskUpdating();
        } else {
            return null;
        }
    }

    /**
     * Set detector level task is updating currently. This is to avoid version conflict
     * caused by multiple entity tasks update detector level task concurrently.
     *
     * @param detectorId detector id
     * @param updating updating or not
     */
    public void setDetectorTaskUpdating(String detectorId, boolean updating) {
        if (hcTaskCaches.containsKey(detectorId)) {
            getExistingHCTaskCache(detectorId).setDetectorTaskUpdating(updating);
        }
    }

    /**
     * Clear pending entities of HC detector.
     *
     * @param detectorId detector id
     */
    public void clearPendingEntities(String detectorId) {
        if (hcTaskCaches.containsKey(detectorId)) {
            hcTaskCaches.get(detectorId).clearPendingEntities();
        }
    }

    /**
     * Check if realtime task field value changed or not by comparing with cache.
     * 1. If new field value is null, will consider this field as not changed.
     * 2. If any field value changed, will consider the realtime task changed.
     * 3. If realtime task cache not found, will consider the realtime task changed.
     *
     * @param detectorId detector id
     * @param newState new task state
     * @param newInitProgress new init progress
     * @param newError new error
     * @return true if realtime task changed comparing with realtime task cache.
     */
    public boolean checkIfRealtimeTaskChanged(String detectorId, String newState, Float newInitProgress, String newError) {
        if (realtimeTaskCaches.containsKey(detectorId)) {
            ADRealtimeTaskCache realtimeTaskCache = realtimeTaskCaches.get(detectorId);
            boolean stateChanged = false;
            if (newState != null && !newState.equals(realtimeTaskCache.getState())) {
                stateChanged = true;
            }
            boolean initProgressChanged = false;
            if (newInitProgress != null && !newInitProgress.equals(realtimeTaskCache.getInitProgress())) {
                initProgressChanged = true;
            }
            boolean errorChanged = false;
            if (newError != null && !newError.equals(realtimeTaskCache.getError())) {
                errorChanged = true;
            }
            if (stateChanged || initProgressChanged || errorChanged) {
                return true;
            }
            return false;
        } else {
            return true;
        }
    }

    /**
     * Update realtime task cache with new field values. If realtime task cache exist, update it
     * directly; otherwise create new realtime task cache.
     *
     * @param detectorId detector id
     * @param newState new task state
     * @param newInitProgress new init progress
     * @param newError new error
     */
    public void updateRealtimeTaskCache(String detectorId, String newState, Float newInitProgress, String newError) {
        if (realtimeTaskCaches.containsKey(detectorId)) {
            ADRealtimeTaskCache realtimeTaskCache = realtimeTaskCaches.get(detectorId);
            if (newState != null) {
                realtimeTaskCache.setState(newState);
            }
            if (newInitProgress != null) {
                realtimeTaskCache.setInitProgress(newInitProgress);
            }
            if (newError != null) {
                realtimeTaskCache.setError(newError);
            }
            logger.debug("update realtime task cache successfully");
        } else {
            realtimeTaskCaches.put(detectorId, new ADRealtimeTaskCache(newState, newInitProgress, newError));
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

}
