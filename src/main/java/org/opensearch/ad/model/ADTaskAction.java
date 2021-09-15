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

package org.opensearch.ad.model;

import org.opensearch.action.ActionListener;
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
}
