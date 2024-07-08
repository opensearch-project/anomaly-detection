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

package org.opensearch.forecast.constant;

public class ForecastCommonName {
    // ======================================
    // Validation
    // ======================================
    // detector validation aspect
    public static final String FORECASTER_ASPECT = "forecaster";

    // ======================================
    // Used for custom forecast result index
    // ======================================
    public static final String DUMMY_FORECAST_RESULT_ID = "dummy_forecast_result_id";
    public static final String DUMMY_FORECASTER_ID = "dummy_forecaster_id";
    public static final String CUSTOM_RESULT_INDEX_PREFIX = "opensearch-forecast-result-";

    // ======================================
    // Index name
    // ======================================
    // index name for forecast checkpoint of each model. One model one document.
    public static final String FORECAST_CHECKPOINT_INDEX_NAME = ".opensearch-forecast-checkpoints";
    // index name for forecast state. Will store forecast task in this index as well.
    public static final String FORECAST_STATE_INDEX = ".opensearch-forecast-state";
    // The alias of the index in which to write forecast result history. Not a hidden index.
    // Allow users to create dashboard or query freely on top of it.
    public static final String FORECAST_RESULT_INDEX_ALIAS = "opensearch-forecast-results";

    // ======================================
    // Used in toXContent
    // ======================================
    public static final String ID_JSON_KEY = "forecasterID";

    // ======================================
    // Used in stats API
    // ======================================
    public static final String FORECASTER_ID_KEY = "forecaster_id";

    // ======================================
    // Historical forecasters
    // ======================================
    public static final String FORECAST_TASK = "forecast_task";
}
