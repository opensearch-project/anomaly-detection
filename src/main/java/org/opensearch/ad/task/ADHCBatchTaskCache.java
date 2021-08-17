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

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.ad.model.ADTaskState;

/**
 * AD HC detector batch task cache which will mainly hold these for HC detector
 * 1. pending entities queue
 * 2. running entities queue
 * 3. temp entities queue
 * 4. task retry times
 * 5. task rate limiters
 *
 * TODO: some methods in this class are not being used currently. Just add them here to save some effort in later PRs.
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

    // HC detector level task updating or not.
    // This is to control only one entity task updating detector level task.
    private Boolean detectorTaskUpdating;

    // Top entities inited or not.
    private Boolean topEntitiesInited;

    // Record how many times the task has retried. Key is task id.
    private Map<String, AtomicInteger> taskRetryTimes;

    // detector level task state
    private String detectorTaskState;

    public ADHCBatchTaskCache() {
        this.pendingEntities = new ConcurrentLinkedQueue<>();
        this.runningEntities = new ConcurrentLinkedQueue<>();
        this.tempEntities = new ConcurrentLinkedQueue<>();
        this.taskRetryTimes = new ConcurrentHashMap<>();
        this.detectorTaskUpdating = false;
        this.topEntitiesInited = false;
        this.detectorTaskState = ADTaskState.INIT.name();
    }

    public void setTopEntityCount(Integer topEntityCount) {
        this.topEntityCount = topEntityCount;
    }

    public Queue<String> getPendingEntities() {
        return pendingEntities;
    }

    public String[] getRunningEntities() {
        return runningEntities.toArray(new String[0]);
    }

    public Integer getTopEntityCount() {
        return topEntityCount;
    }

    public Boolean getDetectorTaskUpdating() {
        return detectorTaskUpdating;
    }

    public void setDetectorTaskUpdating(boolean detectorTaskUpdating) {
        this.detectorTaskUpdating = detectorTaskUpdating;
    }

    public boolean getTopEntitiesInited() {
        return topEntitiesInited;
    }

    public void setEntityTaskLanes(int entityTaskLanes) {
        this.entityTaskLanes = new AtomicInteger(entityTaskLanes);
    }

    public int getAndDecreaseEntityTaskLanes() {
        return this.entityTaskLanes.getAndDecrement();
    }

    public void setTopEntitiesInited(boolean inited) {
        this.topEntitiesInited = inited;
    }

    public int getTaskRetryTimes(String taskId) {
        return taskRetryTimes.computeIfAbsent(taskId, id -> new AtomicInteger(0)).get();
    }

    public String getDetectorTaskState() {
        return detectorTaskState;
    }

    public void setDetectorTaskState(String detectorTaskState) {
        this.detectorTaskState = detectorTaskState;
    }

    /**
     * Add list of entities into pending entity queue. Will check if these entities exists
     * in temp entities queue first. If yes, will remove from temp entities queue.
     * @param entities a list of entity
     */
    public void addEntities(List<String> entities) {
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
        if (entity == null) {
            return;
        }
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

    private void removeFromTempEntity(String entity) {
        if (entity != null && !this.tempEntities.contains(entity)) {
            this.tempEntities.remove(entity);
        }
    }

    public int getPendingEntityCount() {
        return this.pendingEntities.size();
    }

    public int getRunningEntityCount() {
        return this.runningEntities.size();
    }

    public int getTempEntityCount() {
        return this.tempEntities.size();
    }

    public boolean hasEntity() {
        return !this.pendingEntities.isEmpty() || !this.runningEntities.isEmpty() || !this.tempEntities.isEmpty();
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
