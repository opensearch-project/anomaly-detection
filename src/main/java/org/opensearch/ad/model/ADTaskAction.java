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

/**
 * AD task action enum. Have 2 classes of task actions:
 * 1. AD task actions which execute on coordinating node.
 * 2. Task slot actions which execute on lead node.
 */
public enum ADTaskAction {
    // ======================================
    // Actions execute on coordinating node
    // ======================================
    // Start historical analysis for detector
    START,
    // Historical analysis finished
    FINISHED,
    // Cancel historical analysis (single entity detector doesn't need this action, only HC detector use this)
    CANCEL,
    // Run next entity for HC detector historical analysis.
    NEXT_ENTITY,
    // Push back entity to pending entities queue and run next entity.
    PUSH_BACK_ENTITY,
    // Clean stale entities in running entity queue, for example the work node crashed and fail to remove entity
    CLEAN_STALE_RUNNING_ENTITIES,
    // Scale entity task lane
    SCALE_ENTITY_TASK_LANE,

    // ======================================
    // Actions execute on lead node
    // ======================================
    // Check if there is available task slot
    APPLY_FOR_TASK_SLOTS,
    // Check current task slot state and scale
    CHECK_AVAILABLE_TASK_SLOTS,
}
