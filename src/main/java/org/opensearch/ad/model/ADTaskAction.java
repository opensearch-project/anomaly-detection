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

package org.opensearch.ad.model;

import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.TransportService;

/**
 * AD task action enum. Have 2 classes of task actions:
 * <ul>
 *     <li>AD task actions which execute on coordinating node.</li>
 *     <li>Task slot actions which execute on lead node.</li>
 * </ul>
 */
public enum ADTaskAction {
    // ======================================
    // Actions execute on coordinating node
    // ======================================
    /**
     * Start historical analysis for detector.
     * <p>Execute on coordinating node</p>
     */
    START,
    /**
     * Historical analysis finished, so we need to remove detector cache. Used for these cases
     * <ul>
     *     <li>Single entity detector finished/failed/cancelled. Check ADBatchTaskRunner#internalBatchTaskListener</li>
     *     <li>Reset task state as stopped. Check ADTaskManager#resetTaskStateAsStopped</li>
     *     <li>When stop realtime job, we need to stop task and clean up cache. Check ADTaskManager#stopLatestRealtimeTask</li>
     *     <li> When start realtime job, will clean stale cache on old coordinating node.
     *          Check ADTaskManager#initRealtimeTaskCacheAndCleanupStaleCache</li>
     * </ul>
     */
    CLEAN_CACHE,
    /**
     * Cancel historical analysis. Currently only used for HC detector. Single entity detector just need
     * to cancel itself. HC detector need to cancel detector level task on coordinating node.
     * <p>Execute on coordinating node</p>
     */
    CANCEL,
    /**
     * Run next entity for HC detector historical analysis. If no entity, will set detector task as done.
     * <p>Execute on coordinating node</p>
     */
    NEXT_ENTITY,
    /**
     * If any retryable exception happens for HC entity task like limit exceed exception, will push back
     * entity to pending entities queue and run next entity.
     * <p>Execute on coordinating node</p>
     */
    PUSH_BACK_ENTITY,
    /**
     * Clean stale entities in running entity queue, for example the work node crashed and fail to remove
     * entity from running entity queue on coordinating node.
     * <p>Execute on coordinating node</p>
     */
    CLEAN_STALE_RUNNING_ENTITIES,
    /**
     * Scale entity task slots for HC historical analysis.
     * Check {@link org.opensearch.ad.task.ADTaskManager#runNextEntityForHCADHistorical(ADTask, TransportService, ActionListener)}.
     * <p>Execute on coordinating node</p>
     */
    SCALE_ENTITY_TASK_SLOTS,

    // ======================================
    // Actions execute on lead node
    // ======================================
    /**
     * Apply for task slots when historical analysis starts.
     * <p>Execute on lead node</p>
     */
    APPLY_FOR_TASK_SLOTS,
    /**
     * Check current available task slots in cluster. HC historical analysis need this to scale task slots.
     * <p>Execute on lead node</p>
     */
    CHECK_AVAILABLE_TASK_SLOTS,
}
