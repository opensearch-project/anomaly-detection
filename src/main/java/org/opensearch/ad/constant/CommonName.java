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

import org.opensearch.ad.stats.StatNames;

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
    public static final String AD_PLUGIN_NAME = "opensearch-anomaly-detection";
    public static final String AD_PLUGIN_NAME_FOR_TEST = "org.opensearch.ad.AnomalyDetectorPlugin";
    public static final String AD_PLUGIN_VERSION_FOR_TEST = "NA";

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
    public static final String MODEL_SIZE_IN_BYTES = "model_size_in_bytes";
    public static final String CATEGORICAL_FIELD = "category_field";

    public static final String TOTAL_ENTITIES = "total_entities";
    public static final String ACTIVE_ENTITIES = "active_entities";
    public static final String ENTITY_INFO = "entity_info";
    public static final String TOTAL_UPDATES = "total_updates";
    public static final String MODEL_COUNT = StatNames.MODEL_COUNT.getName();
    // ======================================
    // Historical detectors
    // ======================================
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

    // ======================================
    // Used in almost all components
    // ======================================
    public static final String MODEL_ID_KEY = "model_id";
    public static final String DETECTOR_ID_KEY = "detector_id";
    public static final String ENTITY_KEY = "entity";

    // ======================================
    // Used in toXContent
    // ======================================
    public static final String RCF_SCORE_JSON_KEY = "rCFScore";
    public static final String ID_JSON_KEY = "adID";
    public static final String FEATURE_JSON_KEY = "features";
    public static final String CONFIDENCE_JSON_KEY = "confidence";
    public static final String ANOMALY_GRADE_JSON_KEY = "anomalyGrade";
    public static final String QUEUE_JSON_KEY = "queue";
    public static final String START_JSON_KEY = "start";
    public static final String END_JSON_KEY = "end";
    public static final String VALUE_JSON_KEY = "value";
    public static final String ENTITIES_JSON_KEY = "entities";

    // ======================================
    // Used for backward-compatibility in messaging
    // ======================================
    public static final String EMPTY_FIELD = "";
}
