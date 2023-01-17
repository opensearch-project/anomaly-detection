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

package org.opensearch.timeseries.stats;

import java.util.HashSet;
import java.util.Set;

/**
 * Enum containing names of all external stats which will be returned in
 * AD stats REST API.
 */
public enum StatNames {
    // common stats
    CONFIG_INDEX_STATUS("config_index_status", StatType.TIMESERIES),
    JOB_INDEX_STATUS("job_index_status", StatType.TIMESERIES),
    // AD stats
    AD_EXECUTE_REQUEST_COUNT("ad_execute_request_count", StatType.AD),
    AD_EXECUTE_FAIL_COUNT("ad_execute_failure_count", StatType.AD),
    AD_HC_EXECUTE_REQUEST_COUNT("ad_hc_execute_request_count", StatType.AD),
    AD_HC_EXECUTE_FAIL_COUNT("ad_hc_execute_failure_count", StatType.AD),
    DETECTOR_COUNT("detector_count", StatType.AD),
    SINGLE_STREAM_DETECTOR_COUNT("single_stream_detector_count", StatType.AD),
    HC_DETECTOR_COUNT("hc_detector_count", StatType.AD),
    ANOMALY_RESULTS_INDEX_STATUS("anomaly_results_index_status", StatType.AD),
    AD_MODELS_CHECKPOINT_INDEX_STATUS("anomaly_models_checkpoint_index_status", StatType.AD),
    ANOMALY_DETECTION_STATE_STATUS("anomaly_detection_state_status", StatType.AD),
    MODEL_INFORMATION("models", StatType.AD),
    AD_EXECUTING_BATCH_TASK_COUNT("ad_executing_batch_task_count", StatType.AD),
    AD_CANCELED_BATCH_TASK_COUNT("ad_canceled_batch_task_count", StatType.AD),
    AD_TOTAL_BATCH_TASK_EXECUTION_COUNT("ad_total_batch_task_execution_count", StatType.AD),
    AD_BATCH_TASK_FAILURE_COUNT("ad_batch_task_failure_count", StatType.AD),
    MODEL_COUNT("model_count", StatType.AD),
    AD_MODEL_CORRUTPION_COUNT("ad_model_corruption_count", StatType.AD),
    // forecast stats
    FORECAST_EXECUTE_REQUEST_COUNT("forecast_execute_request_count", StatType.FORECAST),
    FORECAST_EXECUTE_FAIL_COUNT("forecast_execute_failure_count", StatType.FORECAST),
    FORECAST_HC_EXECUTE_REQUEST_COUNT("forecast_hc_execute_request_count", StatType.FORECAST),
    FORECAST_HC_EXECUTE_FAIL_COUNT("forecast_hc_execute_failure_count", StatType.FORECAST),
    FORECAST_RESULTS_INDEX_STATUS("forecast_results_index_status", StatType.FORECAST),
    FORECAST_MODELS_CHECKPOINT_INDEX_STATUS("forecast_models_checkpoint_index_status", StatType.FORECAST),
    FORECAST_STATE_STATUS("forecastn_state_status", StatType.FORECAST),
    FORECASTER_COUNT("forecaster_count", StatType.FORECAST),
    SINGLE_STREAM_FORECASTER_COUNT("single_stream_forecaster_count", StatType.FORECAST),
    HC_FORECASTER_COUNT("hc_forecaster_count", StatType.FORECAST),
    FORECAST_MODEL_CORRUTPION_COUNT("forecast_model_corruption_count", StatType.FORECAST);

    private final String name;
    private final StatType type;

    StatNames(String name, StatType type) {
        this.name = name;
        this.type = type;
    }

    /**
     * Get stat name
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    public StatType getType() {
        return type;
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
