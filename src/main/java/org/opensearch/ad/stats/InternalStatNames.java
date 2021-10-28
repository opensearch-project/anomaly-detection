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
