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

package com.amazon.opendistroforelasticsearch.ad.task;

import static com.amazon.opendistroforelasticsearch.ad.MemoryTracker.Origin.HISTORICAL_SINGLE_ENTITY_DETECTOR;
import static com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages.DETECTOR_IS_RUNNING;
import static com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages.EXCEED_HISTORICAL_ANALYSIS_LIMIT;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.NUM_TREES;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.THRESHOLD_MODEL_TRAINING_SIZE;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;

import com.amazon.opendistroforelasticsearch.ad.MemoryTracker;
import com.amazon.opendistroforelasticsearch.ad.common.exception.DuplicateTaskException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.randomcutforest.RandomCutForest;
import com.google.common.collect.ImmutableList;

public class ADTaskCacheManager {
    private final Logger logger = LogManager.getLogger(ADTaskCacheManager.class);
    private final Map<String, ADBatchTaskCache> taskCaches;
    private volatile Integer maxAdBatchTaskPerNode;
    private final MemoryTracker memoryTracker;
    private final int numberSize = 8;
    private final int taskRetryLimit = 3;

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
        taskCaches = new ConcurrentHashMap<>();
        this.memoryTracker = memoryTracker;
        this.detectors = Sets.newConcurrentHashSet();
        this.hcTaskCaches = new ConcurrentHashMap<>();
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
        if (contains(taskId)) {
            throw new DuplicateTaskException(DETECTOR_IS_RUNNING);
        }
        if (containsTaskOfDetector(adTask.getDetectorId())) {
            throw new DuplicateTaskException(DETECTOR_IS_RUNNING);
        }
        checkRunningTaskLimit();
        long neededCacheSize = calculateADTaskCacheSize(adTask);
        if (!memoryTracker.canAllocateReserved(adTask.getDetectorId(), neededCacheSize)) {
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
     * @throws LimitExceededException throw limit exceed exception when the detector id already in cache
     */
    public synchronized void add(String detectorId) {
        if (detectors.contains(detectorId)) {
            logger.debug("detector is already in running detector cache, detectorId: " + detectorId);
            throw new DuplicateTaskException(DETECTOR_IS_RUNNING);
        }
        logger.debug("add detector in running detector cache, detectorId: " + detectorId);
        this.detectors.add(detectorId);
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
        return memoryTracker.estimateModelSize(detector, NUM_TREES) + trainingDataMemorySize(THRESHOLD_MODEL_TRAINING_SIZE)
            + shingleMemorySize(detector.getShingleSize(), detector.getEnabledFeatureIds().size());
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
        return memoryTracker.estimateModelSize(dimensions, numberOfTrees, NUM_SAMPLES_PER_TREE);
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
     * Set max allowed running entities for HC detector.
     *
     * @param detectorId detector id
     * @param allowedRunningEntities max allowed running entities
     */
    public void setAllowedRunningEntities(String detectorId, int allowedRunningEntities) {
        getExistingHCTaskCache(detectorId).setEntityTaskLanes(allowedRunningEntities);
    }

    private ADHCBatchTaskCache getExistingHCTaskCache(String detectorId) {
        if (hcTaskCaches.containsKey(detectorId)) {
            return hcTaskCaches.get(detectorId);
        } else {
            throw new IllegalArgumentException("Can't find HC detector in cache");
        }
    }

    /**
     * Add list of entities into pending entity queue.
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
     * Poll one entity from HC detector entities cache.
     * Will return null if no entities in cache.
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
     * Add one entity into pending entity queue.
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
     * Get current allowed entity task lanes and decrease it by 1.
     *
     * @param detectorId detector id
     * @return current allowed entity task lane count
     */
    public synchronized int getAndDecreaseEntityTaskLanes(String detectorId) {
        return getExistingHCTaskCache(detectorId).getAndDecreaseEntityTaskLanes();
    }

    /**
     * Task exceeds max retry limit or not.
     *
     * @param detectorId detector id
     * @param taskId task id
     * @return true if exceed retry limit; otherwise return false
     */
    public boolean exceedRetryLimit(String detectorId, String taskId) {
        return getExistingHCTaskCache(detectorId).getTaskRetryTimes(taskId) > taskRetryLimit;
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
}
