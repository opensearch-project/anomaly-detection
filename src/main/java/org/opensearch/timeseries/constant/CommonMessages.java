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

    public static final String TOO_MANY_CATEGORICAL_FIELD_ERR_MSG_FORMAT = "We can have only %d categorical field/s.";
    public static String CAN_NOT_FIND_RESULT_INDEX = "Can't find result index ";
    public static String INVALID_CHAR_IN_RESULT_INDEX_NAME =
        "Result index name has invalid character. Valid characters are a-z, 0-9, -(hyphen) and _(underscore)";
    public static String FAIL_TO_VALIDATE = "fail to validate";
    public static String INVALID_TIMESTAMP = "Timestamp field: (%s) must be of type date";
    public static String NON_EXISTENT_TIMESTAMP = "Timestamp field: (%s) is not found in index mapping";
    public static String INVALID_NAME = "Valid characters for name are a-z, A-Z, 0-9, -(hyphen), _(underscore) and .(period)";
    // change this error message to make it compatible with old version's integration(nexus) test
    public static String FAIL_TO_FIND_CONFIG_MSG = "Can't find config with id: ";
    public static final String CAN_NOT_CHANGE_CATEGORY_FIELD = "Can't change category field";
    public static final String CAN_NOT_CHANGE_CUSTOM_RESULT_INDEX = "Can't change custom result index";
    public static final String CATEGORICAL_FIELD_TYPE_ERR_MSG = "A categorical field must be of type keyword or ip.";
    // Modifying message for FEATURE below may break the parseADValidationException method of ValidateAnomalyDetectorTransportAction
    public static final String FEATURE_INVALID_MSG_PREFIX = "Feature has an invalid query";
    public static final String FEATURE_WITH_EMPTY_DATA_MSG = FEATURE_INVALID_MSG_PREFIX + " returning empty aggregated data: ";
    public static final String FEATURE_WITH_INVALID_QUERY_MSG = FEATURE_INVALID_MSG_PREFIX + " causing a runtime exception: ";
    public static final String UNKNOWN_SEARCH_QUERY_EXCEPTION_MSG =
        "Feature has an unknown exception caught while executing the feature query: ";

    // ======================================
    // Index message
    // ======================================
    public static final String CREATE_INDEX_NOT_ACKNOWLEDGED = "Create index %S not acknowledged";
    public static final String SUCCESS_SAVING_RESULT_MSG = "Result saved successfully.";
    public static final String CANNOT_SAVE_RESULT_ERR_MSG = "Cannot save results due to write block.";

    // ======================================
    // Resource constraints
    // ======================================
    public static final String MEMORY_CIRCUIT_BROKEN_ERR_MSG = "AD memory circuit is broken.";

    // ======================================
    // Transport
    // ======================================
    public static final String INVALID_TIMESTAMP_ERR_MSG = "timestamp is invalid";

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
    public static final String MEMORY_LIMIT_EXCEEDED_ERR_MSG = "AD models memory usage exceeds our limit.";

}
