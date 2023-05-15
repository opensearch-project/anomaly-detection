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

import static org.opensearch.forecast.constant.ForecastCommonName.CUSTOM_RESULT_INDEX_PREFIX;

public class ForecastCommonMessages {
    // ======================================
    // Validation message
    // ======================================
    public static String INVALID_FORECAST_INTERVAL = "Forecast interval must be a positive integer";
    public static String NULL_FORECAST_INTERVAL = "Forecast interval should be set";
    public static String INVALID_FORECASTER_NAME =
        "Valid characters for forecaster name are a-z, A-Z, 0-9, -(hyphen), _(underscore) and .(period)";

    // ======================================
    // Resource constraints
    // ======================================
    public static final String DISABLED_ERR_MSG = "Forecast functionality is disabled. To enable update plugins.forecast.enabled to true";

    // ======================================
    // RESTful API
    // ======================================
    public static String FAIL_TO_CREATE_FORECASTER = "Failed to create forecaster";
    public static String FAIL_TO_UPDATE_FORECASTER = "Failed to update forecaster";
    public static String FAIL_TO_FIND_FORECASTER_MSG = "Can not find forecaster with id: ";
    public static final String FORECASTER_ID_MISSING_MSG = "Forecaster ID is missing";
    public static final String INVALID_TIMESTAMP_ERR_MSG = "timestamp is invalid";

    // ======================================
    // Security
    // ======================================
    public static String NO_PERMISSION_TO_ACCESS_FORECASTER = "User does not have permissions to access forecaster: ";
    public static String FAIL_TO_GET_USER_INFO = "Unable to get user information from forecaster ";

    // ======================================
    // Used for custom forecast result index
    // ======================================
    public static String CAN_NOT_FIND_RESULT_INDEX = "Can't find result index ";
    public static String INVALID_RESULT_INDEX_PREFIX = "Result index must start with " + CUSTOM_RESULT_INDEX_PREFIX;
    public static String INVALID_CHAR_IN_RESULT_INDEX_NAME =
        "Result index name has invalid character. Valid characters are a-z, 0-9, -(hyphen) and _(underscore)";

    // ======================================
    // Task
    // ======================================
    public static String FORECASTER_IS_RUNNING = "Forecaster is already running";

    // ======================================
    // transport
    // ======================================
    public static final String FORECAST_ID_MISSING_MSG = "forecaster ID is missing";
}
