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

import java.util.HashSet;
import java.util.Set;

/**
 * Enum containing names of all external stats which will be returned in
 * AD stats REST API.
 */
public enum StatNames {
    AD_EXECUTE_REQUEST_COUNT("ad_execute_request_count"),
    AD_EXECUTE_FAIL_COUNT("ad_execute_failure_count"),
    AD_HC_EXECUTE_REQUEST_COUNT("ad_hc_execute_request_count"),
    AD_HC_EXECUTE_FAIL_COUNT("ad_hc_execute_failure_count"),
    DETECTOR_COUNT("detector_count"),
    SINGLE_ENTITY_DETECTOR_COUNT("single_entity_detector_count"),
    MULTI_ENTITY_DETECTOR_COUNT("multi_entity_detector_count"),
    ANOMALY_DETECTORS_INDEX_STATUS("anomaly_detectors_index_status"),
    ANOMALY_RESULTS_INDEX_STATUS("anomaly_results_index_status"),
    MODELS_CHECKPOINT_INDEX_STATUS("models_checkpoint_index_status"),
    ANOMALY_DETECTION_JOB_INDEX_STATUS("anomaly_detection_job_index_status"),
    ANOMALY_DETECTION_STATE_STATUS("anomaly_detection_state_status"),
    MODEL_INFORMATION("models"),
    AD_EXECUTING_BATCH_TASK_COUNT("ad_executing_batch_task_count"),
    AD_CANCELED_BATCH_TASK_COUNT("ad_canceled_batch_task_count"),
    AD_TOTAL_BATCH_TASK_EXECUTION_COUNT("ad_total_batch_task_execution_count"),
    AD_BATCH_TASK_FAILURE_COUNT("ad_batch_task_failure_count"),
    MODEL_COUNT("model_count");

    private String name;

    StatNames(String name) {
        this.name = name;
    }

    /**
     * Get stat name
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Get set of stat names
     *
     * @return set of stat names
     */
    public static Set<String> getNames() {
        Set<String> names = new HashSet<>();

        for (StatNames statName : StatNames.values()) {
            names.add(statName.getName());
        }
        return names;
    }
}
