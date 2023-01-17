/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.settings;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

public final class ForecastSettings {
    // ======================================
    // config parameters
    // ======================================
    public static final Setting<TimeValue> FORECAST_INTERVAL = Setting
        .positiveTimeSetting(
            "plugins.forecast.default_interval",
            TimeValue.timeValueMinutes(10),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> FORECAST_WINDOW_DELAY = Setting
        .timeSetting(
            "plugins.forecast.default_window_delay",
            TimeValue.timeValueMinutes(0),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // ======================================
    // restful apis
    // ======================================
    public static final Setting<TimeValue> FORECAST_REQUEST_TIMEOUT = Setting
        .positiveTimeSetting(
            "plugins.forecast.request_timeout",
            TimeValue.timeValueSeconds(10),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // ======================================
    // cleanup resouce setting
    // ======================================
    public static final Setting<Boolean> DELETE_FORECAST_RESULT_WHEN_DELETE_FORECASTER = Setting
        .boolSetting(
            "plugins.forecast.delete_forecast_result_when_delete_forecaster",
            false,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // ======================================
    // resource constraint
    // ======================================
    public static final Setting<Integer> MAX_SINGLE_STREAM_FORECASTERS = Setting
        .intSetting("plugins.forecast.max_forecasters", 1000, 0, 10_000, Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Integer> MAX_HC_FORECASTERS = Setting
        .intSetting("plugins.forecast.max_hc_forecasters", 10, 0, 10_000, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // save partial zero-anomaly grade results after indexing pressure reaching the limit
    // Opendistro version has similar setting. I lowered the value to make room
    // for INDEX_PRESSURE_HARD_LIMIT. I don't find a floatSetting that has both default
    // and fallback values. I want users to use the new default value 0.6 instead of 0.8.
    // So do not plan to use the value of legacy setting as fallback.
    public static final Setting<Float> FORECAST_INDEX_PRESSURE_SOFT_LIMIT = Setting
        .floatSetting("plugins.forecast.index_pressure_soft_limit", 0.6f, 0.0f, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // save only error or larger-than-one anomaly grade results after indexing
    // pressure reaching the limit
    // opensearch-only setting
    public static final Setting<Float> FORECAST_INDEX_PRESSURE_HARD_LIMIT = Setting
        .floatSetting("plugins.forecast.index_pressure_hard_limit", 0.9f, 0.0f, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // we only allow single feature forecast now
    public static final int MAX_FORECAST_FEATURES = 1;

    // ======================================
    // Index setting
    // ======================================
    public static int FORECAST_MAX_UPDATE_RETRY_TIMES = 10_000;

    // ======================================
    // Indices
    // ======================================
    public static final Setting<Long> FORECAST_RESULT_HISTORY_MAX_DOCS_PER_SHARD = Setting
        .longSetting(
            "plugins.forecast.forecast_result_history_max_docs_per_shard",
            // Total documents in the primary shards.
            // Note the count is for Lucene docs. Lucene considers a nested
            // doc a doc too. One result on average equals to 4 Lucene docs.
            // A single Lucene doc is roughly 46.8 bytes (measured by experiments).
            // 1.35 billion docs is about 65 GB. One shard can have at most 65 GB.
            // This number in Lucene doc count is used in RolloverRequest#addMaxIndexDocsCondition
            // for adding condition to check if the index has at least numDocs.
            1_350_000_000L,
            0L,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> FORECAST_RESULT_HISTORY_RETENTION_PERIOD = Setting
        .positiveTimeSetting(
            "plugins.forecast.forecast_result_history_retention_period",
            TimeValue.timeValueDays(30),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> FORECAST_RESULT_HISTORY_ROLLOVER_PERIOD = Setting
        .positiveTimeSetting(
            "plugins.forecast.forecast_result_history_rollover_period",
            TimeValue.timeValueHours(12),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final String FORECAST_RESULTS_INDEX_MAPPING_FILE = "mappings/forecast-results.json";
    public static final String FORECAST_STATE_INDEX_MAPPING_FILE = "mappings/forecast-state.json";
    public static final String FORECAST_CHECKPOINT_INDEX_MAPPING_FILE = "mappings/forecast-checkpoint.json";

    // max number of primary shards of a forecast index
    public static final Setting<Integer> FORECAST_MAX_PRIMARY_SHARDS = Setting
        .intSetting("plugins.forecast.max_primary_shards", 10, 0, 200, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // saving checkpoint every 12 hours.
    // To support 1 million entities in 36 data nodes, each node has roughly 28K models.
    // In each hour, we roughly need to save 2400 models. Since each model saving can
    // take about 1 seconds (default value of FORECAST_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS)
    // we can use up to 2400 seconds to finish saving checkpoints.
    public static final Setting<TimeValue> FORECAST_CHECKPOINT_SAVING_FREQ = Setting
        .positiveTimeSetting(
            "plugins.forecast.checkpoint_saving_freq",
            TimeValue.timeValueHours(12),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> FORECAST_CHECKPOINT_TTL = Setting
        .positiveTimeSetting(
            "plugins.forecast.checkpoint_ttl",
            TimeValue.timeValueDays(7),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // ======================================
    // Security
    // ======================================
    public static final Setting<Boolean> FORECAST_FILTER_BY_BACKEND_ROLES = Setting
        .boolSetting("plugins.forecast.filter_by_backend_roles", false, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // ======================================
    // Task
    // ======================================
    public static int MAX_OLD_FORECAST_TASK_DOCS = 1000;

    public static final Setting<Integer> MAX_OLD_TASK_DOCS_PER_FORECASTER = Setting
        .intSetting(
            "plugins.forecast.max_old_task_docs_per_forecaster",
            // One forecast task is roughly 1.5KB for normal case. Suppose task's size
            // is 2KB conservatively. If we store 1000 forecast tasks for one forecaster,
            // that will be 2GB.
            1,
            1, // keep at least 1 old task per forecaster
            MAX_OLD_FORECAST_TASK_DOCS,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // Maximum number of deleted tasks can keep in cache.
    public static final Setting<Integer> MAX_CACHED_DELETED_TASKS = Setting
        .intSetting("plugins.forecast.max_cached_deleted_tasks", 1000, 1, 10_000, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // ======================================
    // rate-limiting queue parameters
    // ======================================
    /**
     * ES recommends bulk size to be 5~15 MB.
     * ref: https://tinyurl.com/3zdbmbwy
     * Assume each checkpoint takes roughly 200KB.  25 requests are of 5 MB.
     */
    public static final Setting<Integer> FORECAST_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE = Setting
        .intSetting("plugins.forecast.checkpoint_write_queue_batch_size", 25, 1, 60, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // expected execution time per checkpoint maintain request. This setting controls
    // the speed of checkpoint maintenance execution. The larger, the faster, and
    // the more performance impact to customers' workload.
    public static final Setting<Integer> FORECAST_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS = Setting
        .intSetting(
            "plugins.forecast.expected_checkpoint_maintain_time_in_millisecs",
            1000,
            0,
            3600000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * Max concurrent checkpoint writes per node
     */
    public static final Setting<Integer> FORECAST_CHECKPOINT_WRITE_QUEUE_CONCURRENCY = Setting
        .intSetting("plugins.forecast.checkpoint_write_queue_concurrency", 2, 1, 10, Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * Max concurrent cold starts per node
     */
    public static final Setting<Integer> FORECAST_COLD_START_QUEUE_CONCURRENCY = Setting
        .intSetting("plugins.forecast.cold_start_queue_concurrency", 1, 1, 10, Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * Max concurrent result writes per node.  Since checkpoint is relatively large
     * (250KB), we have 2 concurrent threads processing the queue.
     */
    public static final Setting<Integer> FORECAST_RESULT_WRITE_QUEUE_CONCURRENCY = Setting
        .intSetting("plugins.forecast.result_write_queue_concurrency", 2, 1, 10, Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * ES recommends bulk size to be 5~15 MB.
     * ref: https://tinyurl.com/3zdbmbwy
     * Assume each result takes roughly 1KB.  5000 requests are of 5 MB.
     */
    public static final Setting<Integer> FORECAST_RESULT_WRITE_QUEUE_BATCH_SIZE = Setting
        .intSetting("plugins.forecast.result_write_queue_batch_size", 5000, 1, 15000, Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * Max concurrent checkpoint reads per node
     */
    public static final Setting<Integer> FORECAST_CHECKPOINT_READ_QUEUE_CONCURRENCY = Setting
        .intSetting("plugins.forecast.checkpoint_read_queue_concurrency", 1, 1, 10, Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * Assume each checkpoint takes roughly 200KB.  25 requests are of 5 MB.
     */
    public static final Setting<Integer> FORECAST_CHECKPOINT_READ_QUEUE_BATCH_SIZE = Setting
        .intSetting("plugins.forecast.checkpoint_read_queue_batch_size", 25, 1, 60, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // expected execution time per cold entity request. This setting controls
    // the speed of cold entity requests execution. The larger, the faster, and
    // the more performance impact to customers' workload.
    public static final Setting<Integer> FORECAST_EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS = Setting
        .intSetting(
            "plugins.forecast.expected_cold_entity_execution_time_in_millisecs",
            3000,
            0,
            3600000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // the percentage of heap usage allowed for queues holding large requests
    // set it to 0 to disable the queue
    public static final Setting<Float> FORECAST_CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.forecast.checkpoint_write_queue_max_heap_percent",
            0.01f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Float> FORECAST_CHECKPOINT_MAINTAIN_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.forecast.checkpoint_maintain_queue_max_heap_percent",
            0.001f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Float> FORECAST_COLD_START_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.forecast.cold_start_queue_max_heap_percent",
            0.001f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Float> FORECAST_RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.forecast.result_write_queue_max_heap_percent",
            0.01f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Float> FORECAST_CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.forecast.checkpoint_read_queue_max_heap_percent",
            0.001f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Float> FORECAST_COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.forecast.cold_entity_queue_max_heap_percent",
            0.001f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // ======================================
    // fault tolerance
    // ======================================
    public static final Setting<TimeValue> FORECAST_BACKOFF_INITIAL_DELAY = Setting
        .positiveTimeSetting(
            "plugins.forecast.backoff_initial_delay",
            TimeValue.timeValueMillis(1000),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> FORECAST_MAX_RETRY_FOR_BACKOFF = Setting
        .intSetting("plugins.forecast.max_retry_for_backoff", 3, 0, Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<TimeValue> FORECAST_BACKOFF_MINUTES = Setting
        .positiveTimeSetting(
            "plugins.forecast.backoff_minutes",
            TimeValue.timeValueMinutes(15),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> FORECAST_MAX_RETRY_FOR_END_RUN_EXCEPTION = Setting
        .intSetting("plugins.forecast.max_retry_for_end_run_exception", 6, 0, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // ======================================
    // cache related parameters
    // ======================================
    /*
     * Opensearch-only setting
     * Each forecaster has its dedicated cache that stores ten entities' states per node for HC
     * and one entity' state per node for single-stream forecaster.
     * A forecaster's hottest entities load their states into the dedicated cache.
     * Other forecasters cannot use space reserved by a forecaster's dedicated cache.
     * DEDICATED_CACHE_SIZE is a setting to make dedicated cache's size flexible.
     * When that setting is changed, if the size decreases, we will release memory
     * if required (e.g., when a user also decreased ForecastSettings.FORECAST_MODEL_MAX_SIZE_PERCENTAGE,
     * the max memory percentage that forecasting plugin can use);
     * if the size increases, we may reject the setting change if we cannot fulfill
     * that request (e.g., when it will uses more memory than allowed for Forecasting).
     *
     * With compact rcf, rcf with 30 trees and shingle size 4 is of 500KB.
     * The recommended max heap size is 32 GB. Even if users use all of the heap
     * for Forecasting, the max number of entity model cannot surpass
     * 3.2 GB/500KB = 3.2 * 10^10 / 5*10^5 = 6.4 * 10 ^4
     * where 3.2 GB is from 10% memory limit of AD plugin.
     * That's why I am using 60_000 as the max limit.
     */
    public static final Setting<Integer> FORECAST_DEDICATED_CACHE_SIZE = Setting
        .intSetting("plugins.forecast.dedicated_cache_size", 10, 0, 60_000, Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Double> FORECAST_MODEL_MAX_SIZE_PERCENTAGE = Setting
        .doubleSetting("plugins.forecast.model_max_size_percent", 0.1, 0, 0.9, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // ======================================
    // pagination setting
    // ======================================
    // pagination size
    public static final Setting<Integer> FORECAST_PAGE_SIZE = Setting
        .intSetting("plugins.forecast.page_size", 1_000, 0, 10_000, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // Increase the value will adding pressure to indexing anomaly results and our feature query
    // OpenSearch-only setting as previous the legacy default is too low (1000)
    public static final Setting<Integer> FORECAST_MAX_ENTITIES_PER_INTERVAL = Setting
        .intSetting(
            "plugins.forecast.max_entities_per_interval",
            1_000_000,
            0,
            2_000_000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // ======================================
    // stats/profile API setting
    // ======================================
    // the max number of models to return per node.
    // the setting is used to limit resource usage due to showing models
    public static final Setting<Integer> FORECAST_MAX_MODEL_SIZE_PER_NODE = Setting
        .intSetting("plugins.forecast.max_model_size_per_node", 100, 1, 10_000, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // ======================================
    // ML
    // ======================================
    public static final int MINIMUM_SHINLE_SIZE = 4;
}
