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

import org.opensearch.timeseries.stats.StatNames;

public class CommonName {

    // ======================================
    // Index mapping
    // ======================================
    // Elastic mapping type
    public static final String MAPPING_TYPE = "_doc";
    // used for updating mapping
    public static final String SCHEMA_VERSION_FIELD = "schema_version";

    // Used to fetch mapping
    public static final String TYPE = "type";
    public static final String KEYWORD_TYPE = "keyword";
    public static final String IP_TYPE = "ip";
    public static final String DATE_TYPE = "date";
    public static final String DATE_NANOS_TYPE = "date_nanos";

    // ======================================
    // Index name
    // ======================================
    // config index. We are reusing ad detector index.
    public static final String CONFIG_INDEX = ".opendistro-anomaly-detectors";

    // job index. We are reusing ad job index.
    public static final String JOB_INDEX = ".opendistro-anomaly-detector-jobs";

    // ======================================
    // Validation
    // ======================================
    public static final String MODEL_ASPECT = "model";
    public static final String CONFIG_ID_MISSING_MSG = "config ID is missing";

    // ======================================
    // Used for custom forecast result index
    // ======================================
    public static final String PROPERTIES = "properties";

    // ======================================
    // Used in toXContent
    // ======================================
    public static final String START_JSON_KEY = "start";
    public static final String END_JSON_KEY = "end";
    public static final String ENTITIES_JSON_KEY = "entities";
    public static final String ENTITY_KEY = "entity";
    public static final String VALUE_JSON_KEY = "value";
    public static final String VALUE_LIST_FIELD = "value_list";
    public static final String FEATURE_DATA_FIELD = "feature_data";
    public static final String DATA_START_TIME_FIELD = "data_start_time";
    public static final String DATA_END_TIME_FIELD = "data_end_time";
    public static final String EXECUTION_START_TIME_FIELD = "execution_start_time";
    public static final String EXECUTION_END_TIME_FIELD = "execution_end_time";
    public static final String ERROR_FIELD = "error";
    public static final String USER_FIELD = "user";
    public static final String CONFIDENCE_FIELD = "confidence";
    public static final String DATA_QUALITY_FIELD = "data_quality";
    // MODEL_ID_FIELD can be used in profile and stats API as well
    public static final String MODEL_ID_FIELD = "model_id";
    public static final String TIMESTAMP = "timestamp";
    public static final String FIELD_MODEL = "model";
    public static final String ANALYSIS_TYPE_FIELD = "analysis_type";
    public static final String ANSWER_FIELD = "answer";
    public static final String RUN_ONCE_FIELD = "run_once";

    // entity sample in checkpoint.
    // kept for bwc purpose
    public static final String ENTITY_SAMPLE = "sp";
    // current key for entity samples
    public static final String SAMPLE_QUEUE = "samples";

    // ======================================
    // Profile name
    // ======================================
    public static final String MODEL_SIZE_IN_BYTES = "model_size_in_bytes";

    // ======================================
    // Used for backward-compatibility in messaging
    // ======================================
    public static final String EMPTY_FIELD = "";

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
    // Used in toXContent
    // ======================================
    public static final String CONFIG_ID_KEY = "config_id";
    public static final String MODEL_ID_KEY = "model_id";
    public static final String TASK_ID_FIELD = "task_id";
    public static final String TASK = "task";
    public static final String ENTITY_ID_FIELD = "entity_id";

    // ======================================
    // plugin info
    // ======================================
    public static final String TIME_SERIES_PLUGIN_NAME = "opensearch-time-series-analytics";
    public static final String TIME_SERIES_PLUGIN_NAME_FOR_TEST = "org.opensearch.timeseries.TimeSeriesAnalyticsPlugin";
    public static final String TIME_SERIES_PLUGIN_VERSION_FOR_TEST = "NA";

    // ======================================
    // Profile name
    // ======================================
    public static final String CATEGORICAL_FIELD = "category_field";
    public static final String STATE = "state";
    public static final String ERROR = "error";
    public static final String COORDINATING_NODE = "coordinating_node";
    // public static final String SHINGLE_SIZE = "shingle_size";
    public static final String TOTAL_SIZE_IN_BYTES = "total_size_in_bytes";
    public static final String MODELS = "models";
    public static final String MODEL = "model";
    public static final String INIT_PROGRESS = "init_progress";
    public static final String TOTAL_ENTITIES = "total_entities";
    public static final String ACTIVE_ENTITIES = "active_entities";
    public static final String ENTITY_INFO = "entity_info";
    public static final String TOTAL_UPDATES = "total_updates";
    public static final String MODEL_COUNT = StatNames.MODEL_COUNT.getName();

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
    // Format name
    // ======================================
    public static final String EPOCH_MILLIS_FORMAT = "epoch_millis";
}
