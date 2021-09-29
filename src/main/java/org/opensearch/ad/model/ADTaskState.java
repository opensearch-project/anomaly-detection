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

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * AD task states.
 * <ul>
 * <li><code>CREATED</code>:
 *     When user start a historical detector, we will create one task to track the detector
 *     execution and set its state as CREATED
 *
 * <li><code>INIT</code>:
 *     After task created, coordinate node will gather all eligible node’s state and dispatch
 *     task to the worker node with lowest load. When the worker node receives the request,
 *     it will set the task state as INIT immediately, then start to run cold start to train
 *     RCF model. We will track the initialization progress in task.
 *     Init_Progress=ModelUpdates/MinSampleSize
 *
 * <li><code>RUNNING</code>:
 *     If RCF model gets enough data points and passed training, it will start to detect data
 *     normally and output positive anomaly scores. Once the RCF model starts to output positive
 *     anomaly score, we will set the task state as RUNNING and init progress as 100%. We will
 *     track task running progress in task: Task_Progress=DetectedPieces/AllPieces
 *
 * <li><code>FINISHED</code>:
 *     When all historical data detected, we set the task state as FINISHED and task progress
 *     as 100%.
 *
 * <li><code>STOPPED</code>:
 *     User can cancel a running task by stopping detector, for example, user want to tune
 *     feature and reran and don’t want current task run any more. When a historical detector
 *     stopped, we will mark the task flag cancelled as true, when run next piece, we will
 *     check this flag and stop the task. Then task stopped, will set its state as STOPPED
 *
 * <li><code>FAILED</code>:
 *     If any exception happen, we will set task state as FAILED
 * </ul>
 */
public enum ADTaskState {
    CREATED,
    INIT,
    RUNNING,
    FAILED,
    STOPPED,
    FINISHED;

    public static List<String> NOT_ENDED_STATES = ImmutableList
        .of(ADTaskState.CREATED.name(), ADTaskState.INIT.name(), ADTaskState.RUNNING.name());
}
