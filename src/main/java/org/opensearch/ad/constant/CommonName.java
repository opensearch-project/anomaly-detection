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

public class CommonName {
    // ======================================
    // Index name
    // ======================================
    // index name for anomaly checkpoint of each model. One model one document.
    public static final String CHECKPOINT_INDEX_NAME = ".opendistro-anomaly-checkpoints";
    // index name for anomaly detection state. Will store AD task in this index as well.
    public static final String DETECTION_STATE_INDEX = ".opendistro-anomaly-detection-state";
    // TODO: move other index name here

    // The alias of the index in which to write AD result history
    public static final String ANOMALY_RESULT_INDEX_ALIAS = ".opendistro-anomaly-results";

    // ======================================
    // Format name
    // ======================================
    public static final String EPOCH_MILLIS_FORMAT = "epoch_millis";

    // ======================================
    // Anomaly Detector name for X-Opaque-Id header
    // ======================================
    public static final String ANOMALY_DETECTOR = "[Anomaly Detector]";

    // ======================================
    // Ultrawarm node attributes
    // ======================================

    // hot node
    public static String HOT_BOX_TYPE = "hot";

    // warm node
    public static String WARM_BOX_TYPE = "warm";

    // box type
    public static final String BOX_TYPE_KEY = "box_type";

    // ======================================
    // Profile name
    // ======================================
    public static final String STATE = "state";
    public static final String ERROR = "error";
    public static final String COORDINATING_NODE = "coordinating_node";
    public static final String SHINGLE_SIZE = "shingle_size";
    public static final String TOTAL_SIZE_IN_BYTES = "total_size_in_bytes";
    public static final String MODELS = "models";
    public static final String MODEL = "model";
    public static final String INIT_PROGRESS = "init_progress";

    public static final String TOTAL_ENTITIES = "total_entities";
    public static final String ACTIVE_ENTITIES = "active_entities";
    public static final String ENTITY_INFO = "entity_info";
    public static final String TOTAL_UPDATES = "total_updates";
    public static final String AD_TASK = "ad_task";
    public static final String HISTORICAL_ANALYSIS = "historical_analysis";
    public static final String AD_TASK_REMOTE = "ad_task_remote";
    public static final String CANCEL_TASK = "cancel_task";

    // ======================================
    // Index mapping
    // ======================================
    // Elastic mapping type
    public static final String MAPPING_TYPE = "_doc";

    // Used to fetch mapping
    public static final String TYPE = "type";
    public static final String KEYWORD_TYPE = "keyword";
    public static final String IP_TYPE = "ip";

    // used for updating mapping
    public static final String SCHEMA_VERSION_FIELD = "schema_version";

    // ======================================
    // Query
    // ======================================
    // Used in finding the max timestamp
    public static final String AGG_NAME_MAX_TIME = "max_timefield";
    // Used in finding the min timestamp
    public static final String AGG_NAME_MIN_TIME = "min_timefield";
    // date histogram aggregation name
    public static final String DATE_HISTOGRAM = "date_histogram";
    // feature aggregation name
    public static final String FEATURE_AGGS = "feature_aggs";
}
