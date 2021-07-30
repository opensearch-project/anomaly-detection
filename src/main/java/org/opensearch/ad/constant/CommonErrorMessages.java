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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.constant;

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

    private static final String TOO_MANY_CATEGORICAL_FIELD_ERR_MSG_FORMAT = "We can have only %d categorical field.";

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
}
