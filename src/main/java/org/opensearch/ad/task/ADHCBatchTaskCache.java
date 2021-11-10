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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AD HC detector batch task cache which will mainly hold these for HC detector on
 * coordinating node.
 * <ul>
 *    <li>pending entities queue</li>
 *    <li>running entities queue</li>
 *    <li>temp entities queue</li>
 *    <li>entity task lanes</li>
 *    <li>top entities count</li>
 *    <li>top entities inited or not</li>
 *    <li>task retry times</li>
 *    <li>detector task update semaphore to control only 1 thread update detector level task</li>
 * </ul>
 */
public class ADHCBatchTaskCache {

    // Cache pending entities.
    private Queue<String> pendingEntities;

    // Cache running entities.
    private Queue<String> runningEntities;

    // Will move entity from pending queue to this temp queue once task dispatched to work node.
    // If fail to dispatch to work node, will move entity from temp queue to pending queue.
    // If work node returns response successfully, will move entity from temp queue to running queue.
    // If we just move entity from pending queue to running queue directly, the running queue can't
    // match the real running task on worker nodes.
    private Queue<String> tempEntities;

    // How many entity task lanes can run concurrently. One entity task lane can run one entity task.
    // Entity lane is a virtual concept which represents one running entity task.
    private AtomicInteger entityTaskLanes;

    // How many top entities totally for this HC task.
    // Will calculate HC task progress with it and profile API needs this.
    private Integer topEntityCount;

    // This is to control only one entity task updating detector level task.
    private Semaphore detectorTaskUpdatingSemaphore;

    // Top entities inited or not.
    private Boolean topEntitiesInited;

    // Record how many times the task has retried. Key is task id.
    private Map<String, AtomicInteger> taskRetryTimes;

    // record last time when HC detector scales entity task slots
    private Instant lastScaleEntityTaskSlotsTime;

    // record lastest HC detector task run time, will use this field to check if task is running or not.
    private Instant latestTaskRunTime;

    public ADHCBatchTaskCache() {
        this.pendingEntities = new ConcurrentLinkedQueue<>();
        this.runningEntities = new ConcurrentLinkedQueue<>();
        this.tempEntities = new ConcurrentLinkedQueue<>();
        this.taskRetryTimes = new ConcurrentHashMap<>();
        this.detectorTaskUpdatingSemaphore = new Semaphore(1);
        this.topEntitiesInited = false;
        this.lastScaleEntityTaskSlotsTime = Instant.now();
        this.latestTaskRunTime = Instant.now();
    }

    public void setTopEntityCount(Integer topEntityCount) {
        this.refreshLatestTaskRunTime();
        this.topEntityCount = topEntityCount;
    }

    public String[] getPendingEntities() {
        return pendingEntities.toArray(new String[0]);
    }

    public String[] getRunningEntities() {
        return runningEntities.toArray(new String[0]);
    }

    public String[] getTempEntities() {
        return tempEntities.toArray(new String[0]);
    }

    public Integer getTopEntityCount() {
        return topEntityCount;
    }

    public boolean tryAcquireTaskUpdatingSemaphore(long timeoutInMillis) throws InterruptedException {
        return detectorTaskUpdatingSemaphore.tryAcquire(timeoutInMillis, TimeUnit.MILLISECONDS);
    }

    public void releaseTaskUpdatingSemaphore() {
        detectorTaskUpdatingSemaphore.release();
    }

    public boolean getTopEntitiesInited() {
        return topEntitiesInited;
    }

    public void setEntityTaskLanes(int entityTaskLanes) {
        this.refreshLatestTaskRunTime();
        this.entityTaskLanes = new AtomicInteger(entityTaskLanes);
    }

    public int getAndDecreaseEntityTaskLanes() {
        return this.entityTaskLanes.getAndDecrement();
    }

    public int getEntityTaskLanes() {
        return this.entityTaskLanes.get();
    }

    public void setTopEntitiesInited(boolean inited) {
        this.topEntitiesInited = inited;
    }

    public int getTaskRetryTimes(String taskId) {
        return taskRetryTimes.computeIfAbsent(taskId, id -> new AtomicInteger(0)).get();
    }

    /**
     * Remove entities from both temp and running entities queue and add list of entities into pending entity queue.
     * @param entities a list of entity
     */
    public void addPendingEntities(List<String> entities) {
        this.refreshLatestTaskRunTime();
        if (entities == null || entities.size() == 0) {
            return;
        }
        for (String entity : entities) {
            if (entity != null) {
                // make sure we delete from temp and running queue first before adding the entity to pending queue
                tempEntities.remove(entity);
                runningEntities.remove(entity);
                if (!pendingEntities.contains(entity)) {
                    pendingEntities.add(entity);
                }
            }
        }
    }

    /**
     * Move entity to running entity queue.
     * @param entity entity value
     */
    public void moveToRunningEntity(String entity) {
        this.refreshLatestTaskRunTime();
        if (entity == null) {
            return;
        }
        boolean removed = this.tempEntities.remove(entity);
        // It's possible that entity was removed before this function. Should check if
        // task in temp queue or not before adding it to running queue.
        if (removed && !this.runningEntities.contains(entity)) {
            this.runningEntities.add(entity);
            // clean it from pending queue to make sure entity only exists in running queue
            this.pendingEntities.remove(entity);
        }
    }

    public int getPendingEntityCount() {
        return this.pendingEntities.size();
    }

    public int getRunningEntityCount() {
        return this.runningEntities.size();
    }

    public int getUnfinishedEntityCount() {
        return this.runningEntities.size() + this.tempEntities.size() + this.pendingEntities.size();
    }

    public int getTempEntityCount() {
        return this.tempEntities.size();
    }

    public Instant getLastScaleEntityTaskSlotsTime() {
        return this.lastScaleEntityTaskSlotsTime;
    }

    public void setLastScaleEntityTaskSlotsTime(Instant lastScaleEntityTaskSlotsTime) {
        this.lastScaleEntityTaskSlotsTime = lastScaleEntityTaskSlotsTime;
    }

    public Instant getLatestTaskRunTime() {
        return latestTaskRunTime;
    }

    public void refreshLatestTaskRunTime() {
        this.latestTaskRunTime = Instant.now();
    }

    public boolean hasEntity() {
        return !this.pendingEntities.isEmpty() || !this.runningEntities.isEmpty() || !this.tempEntities.isEmpty();
    }

    public boolean hasRunningEntity() {
        return !this.runningEntities.isEmpty() || !this.tempEntities.isEmpty();
    }

    public boolean removeRunningEntity(String entity) {
        // In normal case, the entity will be moved to running queue if entity task dispatched
        // to worker node successfully. If failed to dispatch to worker node, it will still stay
        // in temp queue, check ADBatchTaskRunner#workerNodeResponseListener. Then will send
        // entity task done message to coordinating node to move to pending queue if exception
        // is retryable or remove entity from cache if not retryable.
        return this.runningEntities.remove(entity) || this.tempEntities.remove(entity);
    }

    /**
     * Clear pending/running/temp entities queues, task retry times and rate limiter cache.
     */
    public void clear() {
        this.pendingEntities.clear();
        this.runningEntities.clear();
        this.tempEntities.clear();
        this.taskRetryTimes.clear();
    }

    /**
     * Poll one entity from pending entities queue. If entity exists, move it into
     * temp entities queue.
     * @return entity value
     */
    public String pollEntity() {
        this.refreshLatestTaskRunTime();
        String entity = this.pendingEntities.poll();
        if (entity != null && !this.tempEntities.contains(entity)) {
            this.tempEntities.add(entity);
        }
        return entity;
    }

    /**
     * Clear pending entities queue.
     */
    public void clearPendingEntities() {
        this.pendingEntities.clear();
    }

    /**
     * Increase task retry times by 1.
     * @param taskId task id
     * @return current retry time
     */
    public int increaseTaskRetry(String taskId) {
        return this.taskRetryTimes.computeIfAbsent(taskId, id -> new AtomicInteger(0)).getAndIncrement();
    }

    /**
     * Check if entity exists in temp entities queue, pending entities queue or running
     * entities queue. If exists, remove from these queues.
     * @param entity entity value
     * @return true if entity exists and removed
     */
    public boolean removeEntity(String entity) {
        this.refreshLatestTaskRunTime();
        if (entity == null) {
            return false;
        }
        boolean removedFromTempQueue = tempEntities.remove(entity);
        boolean removedFromPendingQueue = pendingEntities.remove(entity);
        boolean removedFromRunningQueue = runningEntities.remove(entity);
        return removedFromTempQueue || removedFromPendingQueue || removedFromRunningQueue;
    }
}
