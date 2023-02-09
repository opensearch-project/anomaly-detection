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

package org.opensearch.ad.constant;

import static org.opensearch.ad.constant.CommonName.CUSTOM_RESULT_INDEX_PREFIX;
import static org.opensearch.ad.model.AnomalyDetector.MAX_RESULT_INDEX_NAME_SIZE;
import static org.opensearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler.MAX_DETECTOR_NAME_SIZE;

import java.util.Locale;

public class CommonErrorMessages {
    public static final String AD_ID_MISSING_MSG = "AD ID is missing";
    public static final String MODEL_ID_MISSING_MSG = "Model ID is missing";
    public static final String WAIT_ERR_MSG = "Exception in waiting for result";
    public static final String HASH_ERR_MSG = "Cannot find an RCF node.  Hashing does not work.";
    public static final String NO_CHECKPOINT_ERR_MSG = "No checkpoints found for model id ";
    public static final String MEMORY_LIMIT_EXCEEDED_ERR_MSG = "AD models memory usage exceeds our limit.";
    public static final String FEATURE_NOT_AVAILABLE_ERR_MSG = "No Feature in current detection window.";
    public static final String MEMORY_CIRCUIT_BROKEN_ERR_MSG = "AD memory circuit is broken.";
    public static final String DISABLED_ERR_MSG = "AD plugin is disabled. To enable update plugins.anomaly_detection.enabled to true";
    public static final String CAN_NOT_CHANGE_CATEGORY_FIELD = "Can't change detector category field";
    public static final String CAN_NOT_CHANGE_RESULT_INDEX = "Can't change detector result index";
    public static final String CREATE_INDEX_NOT_ACKNOWLEDGED = "Create index %S not acknowledged";
    // We need this invalid query tag to show proper error message on frontend
    // refer to AD Dashboard code: https://tinyurl.com/8b5n8hat
    public static final String INVALID_SEARCH_QUERY_MSG = "Invalid search query.";
    public static final String ALL_FEATURES_DISABLED_ERR_MSG =
        "Having trouble querying data because all of your features have been disabled.";
    public static final String INVALID_TIMESTAMP_ERR_MSG = "timestamp is invalid";
    public static String FAIL_TO_PARSE_DETECTOR_MSG = "Fail to parse detector with id: ";
    // change this error message to make it compatible with old version's integration(nexus) test
    public static String FAIL_TO_FIND_DETECTOR_MSG = "Can't find detector with id: ";
    public static String FAIL_TO_GET_PROFILE_MSG = "Fail to get profile for detector ";
    public static String FAIL_TO_GET_TOTAL_ENTITIES = "Failed to get total entities for detector ";
    public static String FAIL_TO_GET_USER_INFO = "Unable to get user information from detector ";
    public static String NO_PERMISSION_TO_ACCESS_DETECTOR = "User does not have permissions to access detector: ";
    public static String CATEGORICAL_FIELD_NUMBER_SURPASSED = "We don't support categorical fields more than ";
    public static String EMPTY_PROFILES_COLLECT = "profiles to collect are missing or invalid";
    public static String FAIL_FETCH_ERR_MSG = "Fail to fetch profile for ";
    public static String DETECTOR_IS_RUNNING = "Detector is already running";
    public static String DETECTOR_MISSING = "Detector is missing";
    public static String AD_TASK_ACTION_MISSING = "AD task action is missing";
    public static final String BUG_RESPONSE = "We might have bugs.";
    public static final String INDEX_NOT_FOUND = "index does not exist";
    public static final String NOT_EXISTENT_VALIDATION_TYPE = "The given validation type doesn't exist";
    public static final String UNSUPPORTED_PROFILE_TYPE = "Unsupported profile types";

    private static final String TOO_MANY_CATEGORICAL_FIELD_ERR_MSG_FORMAT = "We can have only %d categorical field/s.";

    public static String getTooManyCategoricalFieldErr(int limit) {
        return String.format(Locale.ROOT, TOO_MANY_CATEGORICAL_FIELD_ERR_MSG_FORMAT, limit);
    }

    public static final String REQUEST_THROTTLED_MSG = "Request throttled. Please try again later.";
    public static String EMPTY_DETECTOR_NAME = "Detector name should be set";
    public static String NULL_TIME_FIELD = "Time field should be set";
    public static String EMPTY_INDICES = "Indices should be set";
    public static String NULL_DETECTION_INTERVAL = "Detection interval should be set";
    public static String INVALID_SHINGLE_SIZE = "Shingle size must be a positive integer";
    public static String INVALID_DETECTION_INTERVAL = "Detection interval must be a positive integer";
    public static String EXCEED_HISTORICAL_ANALYSIS_LIMIT = "Exceed max historical analysis limit per node";
    public static String NO_ELIGIBLE_NODE_TO_RUN_DETECTOR = "No eligible node to run detector ";
    public static String EMPTY_STALE_RUNNING_ENTITIES = "Empty stale running entities";
    public static String CAN_NOT_FIND_LATEST_TASK = "can't find latest task";
    public static String NO_ENTITY_FOUND = "No entity found";
    public static String HISTORICAL_ANALYSIS_CANCELLED = "Historical analysis cancelled by user";
    public static String HC_DETECTOR_TASK_IS_UPDATING = "HC detector task is updating";
    public static String NEGATIVE_TIME_CONFIGURATION = "should be non-negative";
    public static String INVALID_TIME_CONFIGURATION_UNITS = "Time unit %s is not supported";
    public static String INVALID_DETECTOR_NAME =
        "Valid characters for detector name are a-z, A-Z, 0-9, -(hyphen), _(underscore) and .(period)";
    public static String DUPLICATE_FEATURE_AGGREGATION_NAMES = "Detector has duplicate feature aggregation query names: ";
    public static String INVALID_TIMESTAMP = "Timestamp field: (%s) must be of type date";
    public static String NON_EXISTENT_TIMESTAMP = "Timestamp field: (%s) is not found in index mapping";

    public static String FAIL_TO_GET_DETECTOR = "Fail to get detector";
    public static String FAIL_TO_GET_DETECTOR_INFO = "Fail to get detector info";
    public static String FAIL_TO_CREATE_DETECTOR = "Fail to create detector";
    public static String FAIL_TO_UPDATE_DETECTOR = "Fail to update detector";
    public static String FAIL_TO_PREVIEW_DETECTOR = "Fail to preview detector";
    public static String FAIL_TO_START_DETECTOR = "Fail to start detector";
    public static String FAIL_TO_STOP_DETECTOR = "Fail to stop detector";
    public static String FAIL_TO_DELETE_DETECTOR = "Fail to delete detector";
    public static String FAIL_TO_DELETE_AD_RESULT = "Fail to delete anomaly result";
    public static String FAIL_TO_GET_STATS = "Fail to get stats";
    public static String FAIL_TO_SEARCH = "Fail to search";

    public static String CAN_NOT_FIND_RESULT_INDEX = "Can't find result index ";
    public static String INVALID_RESULT_INDEX_PREFIX = "Result index must start with " + CUSTOM_RESULT_INDEX_PREFIX;
    public static String INVALID_RESULT_INDEX_NAME_SIZE = "Result index name size must contains less than "
        + MAX_RESULT_INDEX_NAME_SIZE
        + " characters";
    public static String INVALID_CHAR_IN_RESULT_INDEX_NAME =
        "Result index name has invalid character. Valid characters are a-z, 0-9, -(hyphen) and _(underscore)";
    public static String INVALID_RESULT_INDEX_MAPPING = "Result index mapping is not correct for index: ";
    public static String INVALID_DETECTOR_NAME_SIZE = "Name should be shortened. The maximum limit is "
        + MAX_DETECTOR_NAME_SIZE
        + " characters.";

    public static String WINDOW_DELAY_REC =
        "Latest seen data point is at least %d minutes ago, consider changing window delay to at least %d minutes.";
    public static String TIME_FIELD_NOT_ENOUGH_HISTORICAL_DATA =
        "There isn't enough historical data found with current timefield selected.";
    public static String DETECTOR_INTERVAL_REC =
        "The selected detector interval might collect sparse data. Consider changing interval length to: ";
    public static String RAW_DATA_TOO_SPARSE =
        "Source index data is potentially too sparse for model training. Consider changing interval length or ingesting more data";
    public static String MODEL_VALIDATION_FAILED_UNEXPECTEDLY = "Model validation experienced issues completing.";
    public static String FILTER_QUERY_TOO_SPARSE = "Data is too sparse after data filter is applied. Consider changing the data filter";
    public static String CATEGORY_FIELD_TOO_SPARSE =
        "Data is most likely too sparse with the given category fields. Consider revising category field/s or ingesting more data ";
    public static String CATEGORY_FIELD_NO_DATA =
        "No entity was found with the given categorical fields. Consider revising category field/s or ingesting more data";
    public static String FEATURE_QUERY_TOO_SPARSE =
        "Data is most likely too sparse when given feature queries are applied. Consider revising feature queries.";
    public static String TIMEOUT_ON_INTERVAL_REC = "Timed out getting interval recommendation";

    // Modifying message for FEATURE below may break the parseADValidationException method of ValidateAnomalyDetectorTransportAction
    public static final String FEATURE_INVALID_MSG_PREFIX = "Feature has an invalid query";
    public static final String FEATURE_WITH_EMPTY_DATA_MSG = FEATURE_INVALID_MSG_PREFIX + " returning empty aggregated data: ";
    public static final String FEATURE_WITH_INVALID_QUERY_MSG = FEATURE_INVALID_MSG_PREFIX + " causing a runtime exception: ";
    public static final String UNKNOWN_SEARCH_QUERY_EXCEPTION_MSG =
        "Feature has an unknown exception caught while executing the feature query: ";
    public static final String VALIDATION_FEATURE_FAILURE = "Validation failed for feature(s) of detector %s";
    public static final String NO_MODEL_ERR_MSG = "No RCF models are available either because RCF"
        + " models are not ready or all nodes are unresponsive or the system might have bugs.";

}
