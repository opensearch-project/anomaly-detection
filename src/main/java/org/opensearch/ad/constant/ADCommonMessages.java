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

import static org.opensearch.ad.constant.ADCommonName.CUSTOM_RESULT_INDEX_PREFIX;

public class ADCommonMessages {
    public static final String AD_ID_MISSING_MSG = "AD ID is missing";
    public static final String MODEL_ID_MISSING_MSG = "Model ID is missing";
    public static final String HASH_ERR_MSG = "Cannot find an RCF node.  Hashing does not work.";
    public static final String NO_CHECKPOINT_ERR_MSG = "No checkpoints found for model id ";
    public static final String FEATURE_NOT_AVAILABLE_ERR_MSG = "No Feature in current detection window.";
    public static final String DISABLED_ERR_MSG =
        "AD functionality is disabled. To enable update plugins.anomaly_detection.enabled to true";
    public static String FAIL_TO_PARSE_DETECTOR_MSG = "Fail to parse detector with id: ";
    public static String FAIL_TO_GET_PROFILE_MSG = "Fail to get profile for detector ";
    public static String FAIL_TO_GET_TOTAL_ENTITIES = "Failed to get total entities for detector ";
    public static String CATEGORICAL_FIELD_NUMBER_SURPASSED = "We don't support categorical fields more than ";
    public static String EMPTY_PROFILES_COLLECT = "profiles to collect are missing or invalid";
    public static String FAIL_FETCH_ERR_MSG = "Fail to fetch profile for ";
    public static String DETECTOR_IS_RUNNING = "Detector is already running";
    public static String DETECTOR_MISSING = "Detector is missing";
    public static String AD_TASK_ACTION_MISSING = "AD task action is missing";
    public static final String INDEX_NOT_FOUND = "index does not exist";
    public static final String NOT_EXISTENT_VALIDATION_TYPE = "The given validation type doesn't exist";
    public static final String UNSUPPORTED_PROFILE_TYPE = "Unsupported profile types";

    public static final String REQUEST_THROTTLED_MSG = "Request throttled. Please try again later.";
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
    public static String INVALID_TIME_CONFIGURATION_UNITS = "Time unit %s is not supported";
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

    public static final String NO_MODEL_ERR_MSG = "No RCF models are available either because RCF"
        + " models are not ready or all nodes are unresponsive or the system might have bugs.";
    public static String INVALID_RESULT_INDEX_PREFIX = "Result index must start with " + CUSTOM_RESULT_INDEX_PREFIX;

}
