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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AD HC detector batch task cache which will mainly hold these for HC detector on
 * coordinating node.
 * 1. pending entities queue
 * 2. running entities queue
 * 3. temp entities queue
 * 4. task retry times
 * 5. task rate limiters
 * 6. top entities inited or not
 * 7. top entities count
 * 8. is current node coordinating node
 * 9. is historical analysis cancelled for this HC detector
 * 10. detector task update semaphore to control only 1 thread update detector level task
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

    public boolean tryAcquireTaskUpdatingSemaphore() {
        return detectorTaskUpdatingSemaphore.tryAcquire();
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
     * Add list of entities into pending entity queue. Will check if these entities exists
     * in temp entities queue first. If yes, will remove from temp entities queue.
     * @param entities a list of entity
     */
    public void addEntities(List<String> entities) {
        this.refreshLatestTaskRunTime();
        if (entities == null || entities.size() == 0) {
            return;
        }
        for (String entity : entities) {
            if (entity != null && tempEntities.contains(entity)) {
                tempEntities.remove(entity);
            }
            if (entity != null && !pendingEntities.contains(entity)) {
                pendingEntities.add(entity);
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
        this.pendingEntities.remove(entity);
        this.tempEntities.remove(entity);
        if (!this.runningEntities.contains(entity)) {
            this.runningEntities.add(entity);
        }
    }

    private void moveToTempEntity(String entity) {
        if (entity != null && !this.tempEntities.contains(entity)) {
            this.tempEntities.add(entity);
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
        return !this.runningEntities.isEmpty();
    }

    public boolean removeRunningEntity(String entity) {
        return this.runningEntities.remove(entity);
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
        if (entity != null) {
            this.moveToTempEntity(entity);
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
     */
    public void removeEntity(String entity) {
        this.refreshLatestTaskRunTime();
        if (entity == null) {
            return;
        }
        if (tempEntities.contains(entity)) {
            tempEntities.remove(entity);
        }
        if (pendingEntities.contains(entity)) {
            pendingEntities.remove(entity);
        }
        if (runningEntities.contains(entity)) {
            runningEntities.remove(entity);
        }
    }
}
