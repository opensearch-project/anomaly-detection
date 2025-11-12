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

package org.opensearch.ad.settings;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

/**
 * AD plugin settings.
 */
public final class AnomalyDetectorSettings {

    private AnomalyDetectorSettings() {}

    public static final int MAX_DETECTOR_UPPER_LIMIT = 10000;
    public static final Setting<Integer> AD_MAX_SINGLE_ENTITY_ANOMALY_DETECTORS = Setting
        .intSetting(
            "plugins.anomaly_detection.max_anomaly_detectors",
            LegacyOpenDistroAnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS,
            0,
            MAX_DETECTOR_UPPER_LIMIT,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> AD_MAX_HC_ANOMALY_DETECTORS = Setting
        .intSetting(
            "plugins.anomaly_detection.max_multi_entity_anomaly_detectors",
            LegacyOpenDistroAnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS,
            0,
            MAX_DETECTOR_UPPER_LIMIT,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> MAX_ANOMALY_FEATURES = Setting
        .intSetting(
            "plugins.anomaly_detection.max_anomaly_features",
            LegacyOpenDistroAnomalyDetectorSettings.MAX_ANOMALY_FEATURES,
            0,
            10,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> AD_REQUEST_TIMEOUT = Setting
        .positiveTimeSetting(
            "plugins.anomaly_detection.request_timeout",
            LegacyOpenDistroAnomalyDetectorSettings.REQUEST_TIMEOUT,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> DETECTION_INTERVAL = Setting
        .positiveTimeSetting(
            "plugins.anomaly_detection.detection_interval",
            LegacyOpenDistroAnomalyDetectorSettings.DETECTION_INTERVAL,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> DETECTION_WINDOW_DELAY = Setting
        .timeSetting(
            "plugins.anomaly_detection.detection_window_delay",
            LegacyOpenDistroAnomalyDetectorSettings.DETECTION_WINDOW_DELAY,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> AD_RESULT_HISTORY_ROLLOVER_PERIOD = Setting
        .positiveTimeSetting(
            "plugins.anomaly_detection.ad_result_history_rollover_period",
            LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // Opensearch-only setting. Doesn't plan to use the value of the legacy setting
    // AD_RESULT_HISTORY_MAX_DOCS as that's too low. If the clusterManager node uses opendistro code,
    // it uses the legacy setting. If the clusterManager node uses opensearch code, it uses the new setting.
    public static final Setting<Long> AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD = Setting
        .longSetting(
            "plugins.anomaly_detection.ad_result_history_max_docs_per_shard",
            // Total documents in the primary shards.
            // Note the count is for Lucene docs. Lucene considers a nested
            // doc a doc too. One result corresponding to 4 Lucene docs.
            // A single Lucene doc is roughly 46.8 bytes (measured by experiments).
            // 1.35 billion docs is about 65 GB. One shard can have at most 65 GB.
            // This number in Lucene doc count is used in RolloverRequest#addMaxIndexDocsCondition
            // for adding condition to check if the index has at least numDocs.
            1_350_000_000L,
            0L,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> AD_RESULT_HISTORY_RETENTION_PERIOD = Setting
        .positiveTimeSetting(
            "plugins.anomaly_detection.ad_result_history_retention_period",
            LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * @deprecated This setting is deprecated because we need to manage fault tolerance for
     * multiple analysis such as AD and forecasting.
     * Use TimeSeriesSettings#MAX_RETRY_FOR_UNRESPONSIVE_NODE instead.
     */
    @Deprecated
    public static final Setting<Integer> AD_MAX_RETRY_FOR_UNRESPONSIVE_NODE = Setting
        .intSetting(
            "plugins.anomaly_detection.max_retry_for_unresponsive_node",
            LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * @deprecated This setting is deprecated because we need to manage fault tolerance for
     * multiple analysis such as AD and forecasting.
     * Use {@link TimeSeriesSettings#COOLDOWN_MINUTES} instead.
     */
    @Deprecated
    public static final Setting<TimeValue> AD_COOLDOWN_MINUTES = Setting
        .positiveTimeSetting(
            "plugins.anomaly_detection.cooldown_minutes",
            LegacyOpenDistroAnomalyDetectorSettings.COOLDOWN_MINUTES,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * @deprecated This setting is deprecated because we need to manage fault tolerance for
     * multiple analysis such as AD and forecasting.
     * Use {@link TimeSeriesSettings#BACKOFF_MINUTES} instead.
     */
    @Deprecated
    public static final Setting<TimeValue> AD_BACKOFF_MINUTES = Setting
        .positiveTimeSetting(
            "plugins.anomaly_detection.backoff_minutes",
            LegacyOpenDistroAnomalyDetectorSettings.BACKOFF_MINUTES,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> AD_BACKOFF_INITIAL_DELAY = Setting
        .positiveTimeSetting(
            "plugins.anomaly_detection.backoff_initial_delay",
            LegacyOpenDistroAnomalyDetectorSettings.BACKOFF_INITIAL_DELAY,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> AD_MAX_RETRY_FOR_BACKOFF = Setting
        .intSetting(
            "plugins.anomaly_detection.max_retry_for_backoff",
            LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> AD_MAX_RETRY_FOR_END_RUN_EXCEPTION = Setting
        .intSetting(
            "plugins.anomaly_detection.max_retry_for_end_run_exception",
            LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_END_RUN_EXCEPTION,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Boolean> AD_FILTER_BY_BACKEND_ROLES = Setting
        .boolSetting(
            "plugins.anomaly_detection.filter_by_backend_roles",
            LegacyOpenDistroAnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Boolean> INSIGHTS_ENABLED = Setting
        .boolSetting("plugins.anomaly_detection.insights_enabled", false, Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final String ANOMALY_RESULTS_INDEX_MAPPING_FILE = "mappings/anomaly-results.json";
    public static final String ANOMALY_DETECTION_STATE_INDEX_MAPPING_FILE = "mappings/anomaly-detection-state.json";
    public static final String CHECKPOINT_INDEX_MAPPING_FILE = "mappings/anomaly-checkpoint.json";
    public static final String INSIGHTS_RESULT_INDEX_MAPPING_FILE = "mappings/insights-results.json";

    // saving checkpoint every 12 hours.
    // To support 1 million entities in 36 data nodes, each node has roughly 28K models.
    // In each hour, we roughly need to save 2400 models. Since each model saving can
    // take about 1 seconds (default value of AD_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS)
    // we can use up to 2400 seconds to finish saving checkpoints.
    public static final Setting<TimeValue> AD_CHECKPOINT_SAVING_FREQ = Setting
        .positiveTimeSetting(
            "plugins.anomaly_detection.checkpoint_saving_freq",
            TimeValue.timeValueHours(12),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> AD_CHECKPOINT_TTL = Setting
        .positiveTimeSetting(
            "plugins.anomaly_detection.checkpoint_ttl",
            TimeValue.timeValueDays(7),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // ======================================
    // ML parameters
    // ======================================
    public static final Setting<Double> AD_MODEL_MAX_SIZE_PERCENTAGE = Setting
        .doubleSetting(
            "plugins.anomaly_detection.model_max_size_percent",
            LegacyOpenDistroAnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE,
            0,
            0.9,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final int THRESHOLD_NUM_LOGNORMAL_QUANTILES = 400;

    public static final int THRESHOLD_DOWNSAMPLES = 5_000;

    public static final long THRESHOLD_MAX_SAMPLES = 50_000;

    // Feature processing
    public static final int MAX_TRAIN_SAMPLE = 24;

    public static final int MAX_SAMPLE_STRIDE = 64;

    public static final int MAX_IMPUTATION_NEIGHBOR_DISTANCE = 2;

    // shingling
    public static final double MAX_SHINGLE_PROPORTION_MISSING = 0.25;

    // Thread pool
    public static final int AD_THEAD_POOL_QUEUE_SIZE = 1000;

    // multi-entity caching
    public static final int MAX_ACTIVE_STATES = 1000;

    // the size of the cache for small states like last cold start time for an entity.
    // At most, we have 10 multi-entity detector and each one can be hit by 1000 different entities each
    // minute. Since these states' life time is hour, we keep its size 10 * 1000 = 10000.
    public static final int MAX_SMALL_STATES = 10000;

    // ======================================
    // cache related parameters
    // ======================================
    /*
     * Opensearch-only setting
     * Each detector has its dedicated cache that stores ten entities' states per node.
     * A detector's hottest entities load their states into the dedicated cache.
     * Other detectors cannot use space reserved by a detector's dedicated cache.
     * DEDICATED_CACHE_SIZE is a setting to make dedicated cache's size flexible.
     * When that setting is changed, if the size decreases, we will release memory
     * if required (e.g., when a user also decreased AnomalyDetectorSettings.AD_MODEL_MAX_SIZE_PERCENTAGE,
     * the max memory percentage that AD can use);
     * if the size increases, we may reject the setting change if we cannot fulfill
     * that request (e.g., when it will uses more memory than allowed for AD).
     *
     * With compact rcf, rcf with 50 trees, 1 base dimension, shingle size 8 is of 400KB.
     * The recommended max heap size is 32 GB. Even if users use all of the heap
     * for AD, the max number of entity model cannot surpass
     * 3.2 GB/500KB = 3.2 * 10^10 / 4*10^5 = 8 * 10 ^4
     * where 3.2 GB is from 10% memory limit of AD plugin.
     * That's why I am using 80_000 as the max limit.
     */
    public static final Setting<Integer> AD_DEDICATED_CACHE_SIZE = Setting
        .intSetting("plugins.anomaly_detection.dedicated_cache_size", 10, 0, 80_000, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // We only keep priority (4 bytes float) in inactive cache. 1 million priorities
    // take up 4 MB.
    public static final int MAX_INACTIVE_ENTITIES = 1_000_000;

    // save partial zero-anomaly grade results after indexing pressure reaching the limit
    // Opendistro version has similar setting. I lowered the value to make room
    // for INDEX_PRESSURE_HARD_LIMIT. I don't find a floatSetting that has both default
    // and fallback values. I want users to use the new default value 0.6 instead of 0.8.
    // So do not plan to use the value of legacy setting as fallback.
    public static final Setting<Float> AD_INDEX_PRESSURE_SOFT_LIMIT = Setting
        .floatSetting(
            "plugins.anomaly_detection.index_pressure_soft_limit",
            0.6f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // save only error or larger-than-one anomaly grade results after indexing
    // pressure reaching the limit
    // opensearch-only setting
    public static final Setting<Float> AD_INDEX_PRESSURE_HARD_LIMIT = Setting
        .floatSetting(
            "plugins.anomaly_detection.index_pressure_hard_limit",
            0.9f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // max number of primary shards of an AD index
    public static final Setting<Integer> AD_MAX_PRIMARY_SHARDS = Setting
        .intSetting(
            "plugins.anomaly_detection.max_primary_shards",
            LegacyOpenDistroAnomalyDetectorSettings.MAX_PRIMARY_SHARDS,
            0,
            200,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // ======================================
    // Historical analysis
    // ======================================
    // Maximum number of batch tasks running on one node.
    // TODO: performance test and tune the setting.
    public static final Setting<Integer> MAX_BATCH_TASK_PER_NODE = Setting
        .intSetting(
            "plugins.anomaly_detection.max_batch_task_per_node",
            LegacyOpenDistroAnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE,
            1,
            100,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // Use TimeSeriesSettings.MAX_CACHED_DELETED_TASKS for both AD and forecasting
    @Deprecated
    // Maximum number of deleted tasks can keep in cache.
    public static final Setting<Integer> MAX_CACHED_DELETED_TASKS = Setting
        .intSetting(
            "plugins.anomaly_detection.max_cached_deleted_tasks",
            1000,
            1,
            10_000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // Maximum number of old AD tasks we can keep.
    public static int MAX_OLD_AD_TASK_DOCS = 1000;
    public static final Setting<Integer> MAX_OLD_AD_TASK_DOCS_PER_DETECTOR = Setting
        .intSetting(
            "plugins.anomaly_detection.max_old_ad_task_docs_per_detector",
            // One AD task is roughly 1.5KB for normal case. Suppose task's size
            // is 2KB conservatively. If we store 1000 AD tasks for one detector,
            // that will be 2GB.
            LegacyOpenDistroAnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
            1, // keep at least 1 old AD task per detector
            MAX_OLD_AD_TASK_DOCS,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> BATCH_TASK_PIECE_SIZE = Setting
        .intSetting(
            "plugins.anomaly_detection.batch_task_piece_size",
            LegacyOpenDistroAnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE,
            1,
            TimeSeriesSettings.MAX_BATCH_TASK_PIECE_SIZE,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> BATCH_TASK_PIECE_INTERVAL_SECONDS = Setting
        .intSetting(
            "plugins.anomaly_detection.batch_task_piece_interval_seconds",
            LegacyOpenDistroAnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS,
            1,
            600,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // Maximum number of entities we support for historical analysis.
    public static final int MAX_TOP_ENTITIES_LIMIT_FOR_HISTORICAL_ANALYSIS = 10_000;
    public static final Setting<Integer> MAX_TOP_ENTITIES_FOR_HISTORICAL_ANALYSIS = Setting
        .intSetting(
            "plugins.anomaly_detection.max_top_entities_for_historical_analysis",
            1000,
            1,
            MAX_TOP_ENTITIES_LIMIT_FOR_HISTORICAL_ANALYSIS,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS = Setting
        .intSetting(
            "plugins.anomaly_detection.max_running_entities_per_detector_for_historical_analysis",
            10,
            1,
            1000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // ======================================
    // rate-limiting queue parameters
    // ======================================
    // the percentage of heap usage allowed for queues holding small requests
    // set it to 0 to disable the queue
    public static final Setting<Float> AD_COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.anomaly_detection.cold_entity_queue_max_heap_percent",
            0.001f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Float> AD_CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.anomaly_detection.checkpoint_read_queue_max_heap_percent",
            0.001f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Float> AD_ENTITY_COLD_START_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.anomaly_detection.entity_cold_start_queue_max_heap_percent",
            0.001f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // the percentage of heap usage allowed for queues holding large requests
    // set it to 0 to disable the queue
    public static final Setting<Float> AD_CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.anomaly_detection.checkpoint_write_queue_max_heap_percent",
            0.01f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Float> AD_RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.anomaly_detection.result_write_queue_max_heap_percent",
            0.01f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Float> AD_CHECKPOINT_MAINTAIN_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.anomaly_detection.checkpoint_maintain_queue_max_heap_percent",
            0.001f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // expected execution time per cold entity request. This setting controls
    // the speed of cold entity requests execution. The larger, the faster, and
    // the more performance impact to customers' workload.
    public static final Setting<Integer> AD_EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS = Setting
        .intSetting(
            "plugins.anomaly_detection.expected_cold_entity_execution_time_in_millisecs",
            3000,
            0,
            3600000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // expected execution time per checkpoint maintain request. This setting controls
    // the speed of checkpoint maintenance execution. The larger, the faster, and
    // the more performance impact to customers' workload.
    public static final Setting<Integer> AD_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS = Setting
        .intSetting(
            "plugins.anomaly_detection.expected_checkpoint_maintain_time_in_millisecs",
            1000,
            0,
            3600000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * Max concurrent entity cold starts per node
     */
    public static final Setting<Integer> AD_ENTITY_COLD_START_QUEUE_CONCURRENCY = Setting
        .intSetting(
            "plugins.anomaly_detection.entity_cold_start_queue_concurrency",
            1,
            1,
            10,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * Max concurrent checkpoint reads per node
     */
    public static final Setting<Integer> AD_CHECKPOINT_READ_QUEUE_CONCURRENCY = Setting
        .intSetting(
            "plugins.anomaly_detection.checkpoint_read_queue_concurrency",
            1,
            1,
            10,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * Max concurrent checkpoint writes per node
     */
    public static final Setting<Integer> AD_CHECKPOINT_WRITE_QUEUE_CONCURRENCY = Setting
        .intSetting(
            "plugins.anomaly_detection.checkpoint_write_queue_concurrency",
            2,
            1,
            10,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * Max concurrent result writes per node.  Since checkpoint is relatively large
     * (250KB), we have 2 concurrent threads processing the queue.
     */
    public static final Setting<Integer> AD_RESULT_WRITE_QUEUE_CONCURRENCY = Setting
        .intSetting(
            "plugins.anomaly_detection.result_write_queue_concurrency",
            2,
            1,
            10,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * Assume each checkpoint takes roughly 200KB.  25 requests are of 5 MB.
     */
    public static final Setting<Integer> AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE = Setting
        .intSetting(
            "plugins.anomaly_detection.checkpoint_read_queue_batch_size",
            25,
            1,
            60,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * ES recommends bulk size to be 5~15 MB.
     * ref: https://tinyurl.com/3zdbmbwy
     * Assume each checkpoint takes roughly 200KB.  25 requests are of 5 MB.
     */
    public static final Setting<Integer> AD_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE = Setting
        .intSetting(
            "plugins.anomaly_detection.checkpoint_write_queue_batch_size",
            25,
            1,
            60,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * ES recommends bulk size to be 5~15 MB.
     * ref: https://tinyurl.com/3zdbmbwy
     * Assume each result takes roughly 1KB.  5000 requests are of 5 MB.
     */
    public static final Setting<Integer> AD_RESULT_WRITE_QUEUE_BATCH_SIZE = Setting
        .intSetting(
            "plugins.anomaly_detection.result_write_queue_batch_size",
            5000,
            1,
            15000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // ======================================
    // pagination setting
    // ======================================
    // pagination size
    public static final Setting<Integer> AD_PAGE_SIZE = Setting
        .intSetting("plugins.anomaly_detection.page_size", 1_000, 0, 10_000, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // Increase the value will adding pressure to indexing anomaly results and our feature query
    // OpenSearch-only setting as previous the legacy default is too low (1000)
    public static final Setting<Integer> AD_MAX_ENTITIES_PER_QUERY = Setting
        .intSetting(
            "plugins.anomaly_detection.max_entities_per_query",
            1_000_000,
            0,
            2_000_000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // ======================================
    // preview setting
    // ======================================
    public static final int MIN_PREVIEW_SIZE = 400; // ok to lower

    public static final double PREVIEW_SAMPLE_RATE = 0.25; // ok to adjust, higher for more data, lower for lower latency

    public static final int MAX_PREVIEW_SAMPLES = 300; // ok to adjust, higher for more data, lower for lower latency

    public static final int MAX_PREVIEW_RESULTS = 1_000; // ok to adjust, higher for more data, lower for lower latency

    // Maximum number of entities retrieved for Preview API
    // Not using legacy value 30 as default.
    // Setting default value to 30 of 2-categorical field detector causes heavy GC
    // (half of the time is GC on my 1GB heap machine). This is because we run concurrent
    // feature aggregations/training/prediction.
    // Default value 5 won't cause heavy GC on an 1-GB heap JVM.
    // Since every entity is likely to give some anomalies, 5 entities are enough.
    public static final Setting<Integer> MAX_ENTITIES_FOR_PREVIEW = Setting
        .intSetting("plugins.anomaly_detection.max_entities_for_preview", 5, 1, 30, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // max concurrent preview to limit resource usage
    public static final Setting<Integer> MAX_CONCURRENT_PREVIEW = Setting
        .intSetting("plugins.anomaly_detection.max_concurrent_preview", 2, 1, 20, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // preview timeout in terms of milliseconds
    public static final long PREVIEW_TIMEOUT_IN_MILLIS = 60_000;

    // ======================================
    // top anomaly result API setting
    // ======================================
    public static final long TOP_ANOMALY_RESULT_TIMEOUT_IN_MILLIS = 60_000;

    // ======================================
    // cleanup resouce setting
    // ======================================
    public static final Setting<Boolean> DELETE_AD_RESULT_WHEN_DELETE_DETECTOR = Setting
        .boolSetting(
            "plugins.anomaly_detection.delete_anomaly_result_when_delete_detector",
            false,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // ======================================
    // stats/profile API setting
    // ======================================
    // the max number of models to return per node.
    // the setting is used to limit resource usage due to showing models
    public static final Setting<Integer> AD_MAX_MODEL_SIZE_PER_NODE = Setting
        .intSetting(
            "plugins.anomaly_detection.max_model_size_per_node",
            100,
            1,
            10_000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final double CONFIG_BUCKET_MINIMUM_SUCCESS_RATE = 0.25;
    // This value is set to decrease the number of times we decrease the interval when recommending a new one
    // The reason we need a max is because user could give an arbitrarly large interval where we don't know even
    // with multiplying the interval down how many intervals will be tried.
    public static final int MAX_TIMES_DECREASING_INTERVAL = 10;
}
