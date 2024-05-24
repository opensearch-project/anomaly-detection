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
    public static String CATEGORICAL_FIELD_NUMBER_SURPASSED = "We don't support categorical fields more than ";
    public static String DETECTOR_IS_RUNNING = "Detector is already running";
    public static String DETECTOR_MISSING = "Detector is missing";
    public static String AD_TASK_ACTION_MISSING = "AD task action is missing";
    public static final String INDEX_NOT_FOUND = "index does not exist";
    public static final String UNSUPPORTED_PROFILE_TYPE = "Unsupported profile types";

    public static final String REQUEST_THROTTLED_MSG = "Request throttled. Please try again later.";
    public static String NULL_DETECTION_INTERVAL = "Detection interval should be set";
    public static String INVALID_SHINGLE_SIZE = "Shingle size must be a positive integer";
    public static String INVALID_DETECTION_INTERVAL = "Detection interval must be a positive integer";
    public static String EXCEED_HISTORICAL_ANALYSIS_LIMIT = "Exceed max historical analysis limit per node";
    public static String NO_ELIGIBLE_NODE_TO_RUN_DETECTOR = "No eligible node to run detector ";
    public static String EMPTY_STALE_RUNNING_ENTITIES = "Empty stale running entities";
    public static String NO_ENTITY_FOUND = "No entity found";
    public static String HISTORICAL_ANALYSIS_CANCELLED = "Historical analysis cancelled by user";
    public static String HC_DETECTOR_TASK_IS_UPDATING = "HC detector task is updating";
    public static String INVALID_TIME_CONFIGURATION_UNITS = "Time unit %s is not supported";
    public static String FAIL_TO_GET_DETECTOR = "Fail to get detector";
    public static String FAIL_TO_CREATE_DETECTOR = "Fail to create detector";
    public static String FAIL_TO_UPDATE_DETECTOR = "Fail to update detector";
    public static String FAIL_TO_PREVIEW_DETECTOR = "Fail to preview detector";
    public static String FAIL_TO_START_DETECTOR = "Fail to start detector";
    public static String FAIL_TO_STOP_DETECTOR = "Fail to stop detector";
    public static String FAIL_TO_DELETE_DETECTOR = "Fail to delete detector";
    public static String FAIL_TO_DELETE_AD_RESULT = "Fail to delete anomaly result";
    public static final String NO_MODEL_ERR_MSG = "No RCF models are available either because RCF"
        + " models are not ready or all nodes are unresponsive or the system might have bugs.";
    public static String INVALID_RESULT_INDEX_PREFIX = "Result index must start with " + CUSTOM_RESULT_INDEX_PREFIX;

}
