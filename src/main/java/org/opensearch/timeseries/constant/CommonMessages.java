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

package org.opensearch.timeseries.constant;

import java.util.Locale;

import org.opensearch.timeseries.settings.TimeSeriesSettings;

public class CommonMessages {
    // ======================================
    // Validation message
    // ======================================
    public static String NEGATIVE_TIME_CONFIGURATION = "should be non-negative";
    public static String INVALID_RESULT_INDEX_MAPPING = "Result index mapping is not correct for index: ";
    public static String EMPTY_NAME = "name should be set";
    public static String NULL_TIME_FIELD = "Time field should be set";
    public static String EMPTY_INDICES = "Indices should be set";

    public static String getTooManyCategoricalFieldErr(int limit) {
        return String.format(Locale.ROOT, CommonMessages.TOO_MANY_CATEGORICAL_FIELD_ERR_MSG_FORMAT, limit);
    }

    public static final String TOO_MANY_CATEGORICAL_FIELD_ERR_MSG_FORMAT =
        "Currently we only support up to %d categorical field/s in order to bound system resource consumption.";

    public static String CAN_NOT_FIND_RESULT_INDEX = "Can't find result index ";
    public static String INVALID_CHAR_IN_RESULT_INDEX_NAME =
        "Result index name has invalid character. Valid characters are a-z, 0-9, -(hyphen) and _(underscore)";
    public static String FAIL_TO_VALIDATE = "failed to validate";
    public static String INVALID_TIMESTAMP = "Timestamp field: (%s) must be of type date";
    public static String NON_EXISTENT_TIMESTAMP_IN_INDEX = "Timestamp field: (%s) is not found in the (%s) index mapping";
    public static String NON_EXISTENT_TIMESTAMP = "Timestamp field: (%s) is not found in index mapping";
    public static String INVALID_NAME = "Valid characters for name are a-z, A-Z, 0-9, -(hyphen), _(underscore) and .(period)";
    // change this error message to make it compatible with old version's integration(nexus) test
    public static String FAIL_TO_FIND_CONFIG_MSG = "Can't find config with id: ";
    public static final String CAN_NOT_CHANGE_CATEGORY_FIELD = "Can't change category field";
    public static final String CAN_NOT_CHANGE_CUSTOM_RESULT_INDEX = "Can't change custom result index";
    public static final String CAN_NOT_CHANGE_FLATTEN_RESULT_INDEX = "Can't change flatten result index";
    public static final String CATEGORICAL_FIELD_TYPE_ERR_MSG = "Categorical field %s must be of type keyword or ip.";
    // Modifying message for FEATURE below may break the parseADValidationException method of ValidateAnomalyDetectorTransportAction
    public static final String FEATURE_INVALID_MSG_PREFIX = "Feature has an invalid query";
    public static final String FEATURE_WITH_EMPTY_DATA_MSG = FEATURE_INVALID_MSG_PREFIX + " returning empty aggregated data: ";
    public static final String FEATURE_WITH_INVALID_QUERY_MSG = FEATURE_INVALID_MSG_PREFIX + " causing a runtime exception: ";
    public static final String UNKNOWN_SEARCH_QUERY_EXCEPTION_MSG =
        "Feature has an unknown exception caught while executing the feature query: ";
    public static String DUPLICATE_FEATURE_AGGREGATION_NAMES = "Config has duplicate feature aggregation query names: ";
    public static String TIME_FIELD_NOT_ENOUGH_HISTORICAL_DATA =
        "There isn't enough historical data found with current timefield selected.";
    public static String CATEGORY_FIELD_TOO_SPARSE =
        "Data is most likely too sparse with the given category fields. Consider revising category field/s or ingesting more data.";
    public static String WINDOW_DELAY_REC =
        "Latest seen data point is at least %d minutes ago. Consider changing window delay to at least %d minutes.";
    public static String INTERVAL_REC = "The selected interval might collect sparse data. Consider changing interval length to: ";
    public static String RAW_DATA_TOO_SPARSE =
        "Source index data is potentially too sparse for model training. Consider changing interval length or ingesting more data";
    public static String MODEL_VALIDATION_FAILED_UNEXPECTEDLY = "Model validation experienced issues completing.";
    public static String FILTER_QUERY_TOO_SPARSE = "Data is too sparse after data filter is applied. Consider changing the data filter";
    public static String CATEGORY_FIELD_NO_DATA =
        "No entity was found with the given categorical fields. Consider revising category field/s or ingesting more data";
    public static String FEATURE_QUERY_TOO_SPARSE =
        "Data is most likely too sparse when given feature queries are applied. Consider revising feature queries";
    public static String TIMEOUT_ON_INTERVAL_REC = "Timed out getting interval recommendation";
    public static final String NOT_EXISTENT_VALIDATION_TYPE = "The given validation type doesn't exist";
    public static final String NOT_EXISTENT_SUGGEST_TYPE = "The given suggest type doesn't exist";
    public static final String DESCRIPTION_LENGTH_TOO_LONG = "Description length is too long. Max length is "
        + TimeSeriesSettings.MAX_DESCRIPTION_LENGTH
        + " characters.";
    public static final String INDEX_NOT_FOUND = "index does not exist";
    public static final String FAIL_TO_GET_MAPPING_MSG = "Fail to get the index mapping of %s";
    public static final String FAIL_TO_GET_MAPPING = "Fail to get the index mapping";
    public static final String TIMESTAMP_VALIDATION_FAILED = "Validation failed for timefield of %s, ";

    public static final String FAIL_TO_GET_CONFIG_MSG = "Fail to get config";

    // ======================================
    // Index message
    // ======================================
    public static final String CREATE_INDEX_NOT_ACKNOWLEDGED = "Create index %S not acknowledged by OpenSearch core";
    public static final String SUCCESS_SAVING_RESULT_MSG = "Result saved successfully.";
    public static final String CANNOT_SAVE_RESULT_ERR_MSG = "Cannot save results due to write block.";

    // ======================================
    // Resource constraints
    // ======================================
    public static final String MEMORY_CIRCUIT_BROKEN_ERR_MSG =
        "The total OpenSearch memory usage exceeds our threshold, opening the memory circuit.";

    // ======================================
    // Transport
    // ======================================
    public static final String INVALID_TIMESTAMP_ERR_MSG = "timestamp is invalid";
    public static String FAIL_TO_DELETE_CONFIG = "Fail to delete config";
    public static String FAIL_TO_GET_CONFIG_INFO = "Fail to get config info";

    // ======================================
    // transport/restful client
    // ======================================
    public static final String WAIT_ERR_MSG = "Exception in waiting for result";
    public static final String ALL_FEATURES_DISABLED_ERR_MSG =
        "Having trouble querying data because all of your features have been disabled.";
    // We need this invalid query tag to show proper error message on frontend
    // refer to AD Dashboard code: https://tinyurl.com/8b5n8hat
    public static final String INVALID_SEARCH_QUERY_MSG = "Invalid search query.";
    public static final String NO_REQUESTS_ADDED_ERR = "no requests added";

    // ======================================
    // rate limiting worker
    // ======================================
    public static final String BUG_RESPONSE = "We might have bugs.";
    public static final String MEMORY_LIMIT_EXCEEDED_ERR_MSG = "Models memory usage exceeds our limit.";

    // ======================================
    // security
    // ======================================
    public static String NO_PERMISSION_TO_ACCESS_CONFIG = "User does not have permissions to access config: ";
    public static String FAIL_TO_GET_USER_INFO = "Unable to get user information from config ";

    // ======================================
    // transport
    // ======================================
    public static final String CONFIG_ID_MISSING_MSG = "config ID is missing";
    public static final String MODEL_ID_MISSING_MSG = "model ID is missing";

    // ======================================
    // task
    // ======================================
    public static String CAN_NOT_FIND_LATEST_TASK = "can't find latest task";

    // ======================================
    // Job
    // ======================================
    public static String CONFIG_IS_RUNNING = "Config is already running";
    public static String FAIL_TO_SEARCH = "Fail to search";

    // ======================================
    // Profile API
    // ======================================
    public static String EMPTY_PROFILES_COLLECT = "profiles to collect are missing or invalid";
    public static String FAIL_TO_PARSE_CONFIG_MSG = "Fail to parse config with id: ";
    public static String FAIL_FETCH_ERR_MSG = "Fail to fetch profile for ";
    public static String FAIL_TO_GET_PROFILE_MSG = "Fail to get profile for config ";
    public static String FAIL_TO_GET_TOTAL_ENTITIES = "Failed to get total entities for config ";

    // ======================================
    // Stats API
    // ======================================
    public static String FAIL_TO_GET_STATS = "Fail to get stats";

    // ======================================
    // Suggest API
    // ======================================
    public static String FAIL_SUGGEST_ERR_MSG = "Fail to suggest parameters for ";
}
