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

package org.opensearch.ad.stats;

/**
 * Enum containing names of all internal stats which will not be returned
 * in AD stats REST API.
 */
public enum InternalStatNames {
    JVM_HEAP_USAGE("jvm_heap_usage"),
    AD_USED_BATCH_TASK_SLOT_COUNT("ad_used_batch_task_slot_count"),
    AD_DETECTOR_ASSIGNED_BATCH_TASK_SLOT_COUNT("ad_detector_assigned_batch_task_slot_count");

    private String name;

    InternalStatNames(String name) {
        this.name = name;
    }

    /**
     * Get internal stat name
     *
     * @return name
     */
    public String getName() {
        return name;
    }
}
