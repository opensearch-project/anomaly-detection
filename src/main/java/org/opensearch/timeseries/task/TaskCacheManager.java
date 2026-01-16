/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.task;

import static org.opensearch.timeseries.settings.TimeSeriesSettings.MAX_CACHED_DELETED_TASKS;

import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.transport.TransportService;

public class TaskCacheManager {
    private final Logger logger = LogManager.getLogger(TaskCacheManager.class);
    /**
     * This field is to cache all realtime tasks on coordinating node.
     * <p>Node: coordinating node</p>
     * <p>Key is config id</p>
     */
    private Map<String, RealtimeTaskCache> realtimeTaskCaches;

    /**
     * This field is to cache all deleted config level tasks on coordinating node.
     * Will try to clean up child task and result later.
     * <p>Node: coordinating node</p>
     * Check {@link ForecastTaskManager#cleanChildTasksAndResultsOfDeletedTask()}
     */
    private Queue<String> deletedTasks;

    protected volatile Integer maxCachedDeletedTask;
    /**
     * This field is to cache deleted detector IDs. Hourly cron will poll this queue
     * and clean AD results. Check {@link ADTaskManager#cleanResultOfDeletedConfig}
     * <p>Node: any data node servers delete detector request</p>
     */
    protected Queue<String> deletedConfigs;

    public TaskCacheManager(Settings settings, ClusterService clusterService) {
        this.realtimeTaskCaches = new ConcurrentHashMap<>();
        this.deletedTasks = new ConcurrentLinkedQueue<>();
        this.maxCachedDeletedTask = MAX_CACHED_DELETED_TASKS.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_CACHED_DELETED_TASKS, it -> maxCachedDeletedTask = it);
        this.deletedConfigs = new ConcurrentLinkedQueue<>();
    }

    public RealtimeTaskCache getRealtimeTaskCache(String configId) {
        return realtimeTaskCaches.get(configId);
    }

    public void initRealtimeTaskCache(String configId, long configIntervalInMillis) {
        realtimeTaskCaches.put(configId, new RealtimeTaskCache(null, null, null, configIntervalInMillis));
        logger.debug("Realtime task cache inited");
    }

    /**
     * Add deleted task's id to deleted tasks queue.
     * @param taskId task id
     */
    public void addDeletedTask(String taskId) {
        if (deletedTasks.size() < maxCachedDeletedTask) {
            deletedTasks.add(taskId);
        }
    }

    /**
     * Check if deleted task queue has items.
     * @return true if has deleted task in cache
     */
    public boolean hasDeletedTask() {
        return !deletedTasks.isEmpty();
    }

    /**
     * Poll one deleted task.
     * @return task id
     */
    public String pollDeletedTask() {
        return this.deletedTasks.poll();
    }

    /**
     * Clear realtime task cache.
     */
    public void clearRealtimeTaskCache() {
        realtimeTaskCaches.clear();
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
            RealtimeTaskCache realtimeTaskCache = realtimeTaskCaches.get(detectorId);
            boolean stateChangeNeeded = false;
            String oldState = realtimeTaskCache.getState();
            if (newState != null
                && !newState.equals(oldState)
                && !(TaskState.INIT.name().equals(newState) && TaskState.RUNNING.name().equals(oldState))) {
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
     *
     * Check {@link TaskManager#initRealtimeTaskCacheAndCleanupStaleCache(String, Config, TransportService, ActionListener)}
     *
     * @param configId detector id
     * @param newState new task state
     * @param newInitProgress new init progress
     * @param newError new error
     */
    public void updateRealtimeTaskCache(String configId, String newState, Float newInitProgress, String newError) {
        RealtimeTaskCache realtimeTaskCache = realtimeTaskCaches.get(configId);
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
            if (newState != null && !TaskState.NOT_ENDED_STATES.contains(newState)) {
                // If task is done, will remove its realtime task cache.
                logger.info("Realtime task done with state {}, remove RT task cache for config {}", newState, configId);
                removeRealtimeTaskCache(configId);
            }
        } else {
            logger.debug("Realtime task cache is not inited yet for config {}", configId);
        }
    }

    public void refreshRealtimeJobRunTime(String configId) {
        RealtimeTaskCache taskCache = realtimeTaskCaches.get(configId);
        if (taskCache != null) {
            taskCache.setLastJobRunTime(Instant.now().toEpochMilli());
        }
    }

    /**
     * Get config IDs from realtime task cache.
     * @return array of config id
     */
    public String[] getConfigIdsInRealtimeTaskCache() {
        return realtimeTaskCaches.keySet().toArray(new String[0]);
    }

    /**
     * Remove detector's realtime task from cache.
     * @param configId config id
     */
    public void removeRealtimeTaskCache(String configId) {
        if (realtimeTaskCaches.containsKey(configId)) {
            logger.info("Delete realtime cache for config {}", configId);
            realtimeTaskCaches.remove(configId);
        }
    }

    /**
     * We query result index to check if there are any result generated for config to tell whether it passed initialization of not.
     * To avoid repeated query when there is no data, record whether we have done that or not.
     * @param configId config id
     */
    public void markResultIndexQueried(String configId) {
        RealtimeTaskCache realtimeTaskCache = realtimeTaskCaches.get(configId);
        // we initialize a real time cache at the beginning of AnomalyResultTransportAction if it
        // cannot be found. If the cache is empty, we will return early and wait it for it to be
        // initialized.
        if (realtimeTaskCache != null) {
            realtimeTaskCache.setQueriedResultIndex(true);
        }
    }

    /**
     * We query result index to check if there are any result generated for config to tell whether it passed initialization of not.
     *
     * @param configId config id
     * @return whether we have queried result index or not.
     */
    public boolean hasQueriedResultIndex(String configId) {
        RealtimeTaskCache realtimeTaskCache = realtimeTaskCaches.get(configId);
        if (realtimeTaskCache != null) {
            return realtimeTaskCache.hasQueriedResultIndex();
        }
        return false;
    }

    /**
     * Add deleted config's id to deleted config queue.
     * @param configId config id
     */
    public void addDeletedConfig(String configId) {
        if (deletedConfigs.size() < maxCachedDeletedTask) {
            deletedConfigs.add(configId);
        }
    }

    /**
     * Poll one deleted config.
     * @return config id
     */
    public String pollDeletedConfig() {
        return this.deletedConfigs.poll();
    }
}
