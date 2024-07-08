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

/**
 * Legacy OpenDistro AD plugin settings.
 */
public class LegacyOpenDistroAnomalyDetectorSettings {

    private LegacyOpenDistroAnomalyDetectorSettings() {}

    public static final Setting<Integer> MAX_SINGLE_ENTITY_ANOMALY_DETECTORS = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_anomaly_detectors",
            1000,
            0,
            10_000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<Integer> MAX_MULTI_ENTITY_ANOMALY_DETECTORS = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_multi_entity_anomaly_detectors",
            10,
            0,
            10_000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<Integer> MAX_ANOMALY_FEATURES = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_anomaly_features",
            5,
            0,
            100,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    // give enough timeout to process a single page (max 1000 entities)
    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.request_timeout",
            TimeValue.timeValueSeconds(60),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<TimeValue> DETECTION_INTERVAL = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.detection_interval",
            TimeValue.timeValueMinutes(10),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<TimeValue> DETECTION_WINDOW_DELAY = Setting
        .timeSetting(
            "opendistro.anomaly_detection.detection_window_delay",
            TimeValue.timeValueMinutes(0),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<TimeValue> AD_RESULT_HISTORY_ROLLOVER_PERIOD = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.ad_result_history_rollover_period",
            TimeValue.timeValueHours(12),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<Long> AD_RESULT_HISTORY_MAX_DOCS = Setting
        .longSetting(
            "opendistro.anomaly_detection.ad_result_history_max_docs",
            // Total documents in primary replica.
            // A single feature result is roughly 150 bytes. Suppose a doc is
            // of 200 bytes, 250 million docs is of 50 GB. We choose 50 GB
            // because we have 1 shard at least. One shard can have at most 50 GB.
            250_000_000L,
            0L,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<TimeValue> AD_RESULT_HISTORY_RETENTION_PERIOD = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.ad_result_history_retention_period",
            TimeValue.timeValueDays(30),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<Integer> MAX_RETRY_FOR_UNRESPONSIVE_NODE = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_retry_for_unresponsive_node",
            5,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<TimeValue> COOLDOWN_MINUTES = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.cooldown_minutes",
            TimeValue.timeValueMinutes(5),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<TimeValue> BACKOFF_MINUTES = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.backoff_minutes",
            TimeValue.timeValueMinutes(15),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<TimeValue> BACKOFF_INITIAL_DELAY = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.backoff_initial_delay",
            TimeValue.timeValueMillis(1000),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<Integer> MAX_RETRY_FOR_BACKOFF = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_retry_for_backoff",
            3,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<Integer> MAX_RETRY_FOR_END_RUN_EXCEPTION = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_retry_for_end_run_exception",
            6,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<Boolean> AD_FILTER_BY_BACKEND_ROLES = Setting
        .boolSetting(
            "opendistro.anomaly_detection.filter_by_backend_roles",
            false,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    // ======================================
    // ML parameters
    // ======================================
    // RCF
    public static final Setting<Double> MODEL_MAX_SIZE_PERCENTAGE = Setting
        .doubleSetting(
            "opendistro.anomaly_detection.model_max_size_percent",
            0.1,
            0,
            0.7,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    // Increase the value will adding pressure to indexing anomaly results and our feature query
    public static final Setting<Integer> MAX_ENTITIES_PER_QUERY = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_entities_per_query",
            1000,
            1,
            100_000_000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    // Default number of entities retrieved for Preview API
    public static final int DEFAULT_ENTITIES_FOR_PREVIEW = 30;

    // Maximum number of entities retrieved for Preview API
    public static final Setting<Integer> MAX_ENTITIES_FOR_PREVIEW = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_entities_for_preview",
            DEFAULT_ENTITIES_FOR_PREVIEW,
            1,
            1000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    // save partial zero-anomaly grade results after indexing pressure reaching the limit
    public static final Setting<Float> INDEX_PRESSURE_SOFT_LIMIT = Setting
        .floatSetting(
            "opendistro.anomaly_detection.index_pressure_soft_limit",
            0.8f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    // max number of primary shards of an AD index
    public static final Setting<Integer> MAX_PRIMARY_SHARDS = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_primary_shards",
            10,
            0,
            200,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    // responding to 100 cache misses per second allowed.
    // 100 because the get threadpool (the one we need to get checkpoint) queue szie is 1000
    // and we may have 10 concurrent multi-entity detectors. So each detector can use: 1000 / 10 = 100
    // for 1m interval. if the max entity number is 3000 per node, it will need around 30m to get all of them cached
    // Thus, for 5m internval, it will need 2.5 hours to cache all of them. for 1hour interval, it will be 30hours.
    // but for 1 day interval, it will be 30 days.
    public static Setting<Integer> MAX_CACHE_MISS_HANDLING_PER_SECOND = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_cache_miss_handling_per_second",
            100,
            0,
            1000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    // Maximum number of batch tasks running on one node.
    // TODO: performance test and tune the setting.
    public static final Setting<Integer> MAX_BATCH_TASK_PER_NODE = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_batch_task_per_node",
            10,
            1,
            100,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<Integer> MAX_OLD_AD_TASK_DOCS_PER_DETECTOR = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_old_ad_task_docs_per_detector",
            // One AD task is roughly 1.5KB for normal case. Suppose task's size
            // is 2KB conservatively. If we store 1000 AD tasks for one detector,
            // and have 1000 detectors, that will be 2GB.
            1, // keep 1 old task by default to avoid consuming too much resource
            1, // keep at least 1 old AD task per detector
            1000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<Integer> BATCH_TASK_PIECE_SIZE = Setting
        .intSetting(
            "opendistro.anomaly_detection.batch_task_piece_size",
            1000,
            1,
            10_000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );

    public static final Setting<Integer> BATCH_TASK_PIECE_INTERVAL_SECONDS = Setting
        .intSetting(
            "opendistro.anomaly_detection.batch_task_piece_interval_seconds",
            5,
            1,
            600,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic,
            Setting.Property.Deprecated
        );
}
