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

package org.opensearch.ad.settings;

import java.time.Duration;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

/**
 * AD plugin settings.
 */
public final class AnomalyDetectorSettings {

    private AnomalyDetectorSettings() {}

    public static final Setting<Integer> MAX_SINGLE_ENTITY_ANOMALY_DETECTORS = Setting
        .intSetting(
            "plugins.anomaly_detection.max_anomaly_detectors",
            LegacyOpenDistroAnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS,
            0,
            10_000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> MAX_MULTI_ENTITY_ANOMALY_DETECTORS = Setting
        .intSetting(
            "plugins.anomaly_detection.max_multi_entity_anomaly_detectors",
            LegacyOpenDistroAnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS,
            0,
            10_000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> MAX_ANOMALY_FEATURES = Setting
        .intSetting(
            "plugins.anomaly_detection.max_anomaly_features",
            LegacyOpenDistroAnomalyDetectorSettings.MAX_ANOMALY_FEATURES,
            0,
            100,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting
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
    // AD_RESULT_HISTORY_MAX_DOCS as that's too low. If the master node uses opendistro code,
    // it uses the legacy setting. If the master node uses opensearch code, it uses the new setting.
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

    public static final Setting<Integer> MAX_RETRY_FOR_UNRESPONSIVE_NODE = Setting
        .intSetting(
            "plugins.anomaly_detection.max_retry_for_unresponsive_node",
            LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> COOLDOWN_MINUTES = Setting
        .positiveTimeSetting(
            "plugins.anomaly_detection.cooldown_minutes",
            LegacyOpenDistroAnomalyDetectorSettings.COOLDOWN_MINUTES,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> BACKOFF_MINUTES = Setting
        .positiveTimeSetting(
            "plugins.anomaly_detection.backoff_minutes",
            LegacyOpenDistroAnomalyDetectorSettings.BACKOFF_MINUTES,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> BACKOFF_INITIAL_DELAY = Setting
        .positiveTimeSetting(
            "plugins.anomaly_detection.backoff_initial_delay",
            LegacyOpenDistroAnomalyDetectorSettings.BACKOFF_INITIAL_DELAY,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> MAX_RETRY_FOR_BACKOFF = Setting
        .intSetting(
            "plugins.anomaly_detection.max_retry_for_backoff",
            LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> MAX_RETRY_FOR_END_RUN_EXCEPTION = Setting
        .intSetting(
            "plugins.anomaly_detection.max_retry_for_end_run_exception",
            LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_END_RUN_EXCEPTION,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Boolean> FILTER_BY_BACKEND_ROLES = Setting
        .boolSetting(
            "plugins.anomaly_detection.filter_by_backend_roles",
            LegacyOpenDistroAnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final String ANOMALY_DETECTORS_INDEX_MAPPING_FILE = "mappings/anomaly-detectors.json";
    public static final String ANOMALY_DETECTOR_JOBS_INDEX_MAPPING_FILE = "mappings/anomaly-detector-jobs.json";
    public static final String ANOMALY_RESULTS_INDEX_MAPPING_FILE = "mappings/anomaly-results.json";
    public static final String ANOMALY_DETECTION_STATE_INDEX_MAPPING_FILE = "mappings/anomaly-detection-state.json";
    public static final String CHECKPOINT_INDEX_MAPPING_FILE = "mappings/checkpoint.json";

    public static final Duration HOURLY_MAINTENANCE = Duration.ofHours(1);

    public static final Duration CHECKPOINT_TTL = Duration.ofDays(3);

    // ======================================
    // ML parameters
    // ======================================
    // RCF
    public static final int NUM_SAMPLES_PER_TREE = 256;

    public static final int NUM_TREES = 30;

    public static final int TRAINING_SAMPLE_INTERVAL = 64;

    public static final double TIME_DECAY = 0.0001;

    public static final int NUM_MIN_SAMPLES = 128;

    public static final double DESIRED_MODEL_SIZE_PERCENTAGE = 0.0002;

    public static final Setting<Double> MODEL_MAX_SIZE_PERCENTAGE = Setting
        .doubleSetting(
            "plugins.anomaly_detection.model_max_size_percent",
            LegacyOpenDistroAnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE,
            0,
            0.7,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // for a batch operation, we want all of the bounding box in-place for speed
    public static final double BATCH_BOUNDING_BOX_CACHE_RATIO = 1;

    // for a real-time operation, we trade off speed for memory as real time opearation
    // only has to do one update/scoring per interval
    public static final double REAL_TIME_BOUNDING_BOX_CACHE_RATIO = 0;

    public static final int DEFAULT_SHINGLE_SIZE = 8;

    // Multi-entity detector model setting:
    // TODO (kaituo): change to 4
    public static final int DEFAULT_MULTI_ENTITY_SHINGLE = 1;

    public static final int MAX_SHINGLE_SIZE = 1024;

    // Thresholding
    public static final double THRESHOLD_MIN_PVALUE = 0.995;

    public static final double THRESHOLD_MAX_RANK_ERROR = 0.0001;

    public static final double THRESHOLD_MAX_SCORE = 8;

    public static final int THRESHOLD_NUM_LOGNORMAL_QUANTILES = 400;

    public static final int THRESHOLD_DOWNSAMPLES = 5_000;

    public static final long THRESHOLD_MAX_SAMPLES = 50_000;

    // Feature processing
    public static final int MAX_TRAIN_SAMPLE = 24;

    public static final int MAX_SAMPLE_STRIDE = 64;

    public static final int TRAIN_SAMPLE_TIME_RANGE_IN_HOURS = 24;

    public static final int MIN_TRAIN_SAMPLES = 512;

    public static final int MAX_IMPUTATION_NEIGHBOR_DISTANCE = 2;

    // shingling
    public static final double MAX_SHINGLE_PROPORTION_MISSING = 0.25;

    // AD JOB
    public static final long DEFAULT_AD_JOB_LOC_DURATION_SECONDS = 60;

    // Thread pool
    public static final int AD_THEAD_POOL_QUEUE_SIZE = 1000;

    // multi-entity caching
    public static final int MAX_ACTIVE_STATES = 1000;

    // the size of the cache for small states like last cold start time for an entity.
    // At most, we have 10 multi-entity detector and each one can be hit by 1000 different entities each
    // minute. Since these states' life time is hour, we keep its size 10 * 1000 = 10000.
    public static final int MAX_SMALL_STATES = 10000;

    // how many categorical fields we support
    public static final int CATEGORY_FIELD_LIMIT = 1;

    public static final int MULTI_ENTITY_NUM_TREES = 30;

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
     * if required (e.g., when a user also decreased AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE,
     * the max memory percentage that AD can use);
     * if the size increases, we may reject the setting change if we cannot fulfill
     * that request (e.g., when it will uses more memory than allowed for AD).
     *
     * With compact rcf, rcf with 30 trees and shingle size 4 is of 500KB.
     * The recommended max heap size is 32 GB. Even if users use all of the heap
     * for AD, the max number of entity model cannot surpass
     * 3.2 GB/500KB = 3.2 * 10^10 / 5*10^5 = 6.4 * 10 ^4
     * where 3.2 GB is from 10% memory limit of AD plugin.
     * That's why I am using 60_000 as the max limit.
     */
    public static final Setting<Integer> DEDICATED_CACHE_SIZE = Setting
        .intSetting("plugins.anomaly_detection.dedicated_cache_size", 10, 0, 60_000, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // We only keep priority (4 bytes float) in inactive cache. 1 million priorities
    // take up 4 MB.
    public static final int MAX_INACTIVE_ENTITIES = 1_000_000;

    // 1 million insertion costs roughly 1 MB.
    public static final int DOOR_KEEPER_FOR_CACHE_MAX_INSERTION = 1_000_000;

    // 100,000 insertions costs roughly 1KB.
    public static final int DOOR_KEEPER_FOR_COLD_STARTER_MAX_INSERTION = 100_000;

    public static final double DOOR_KEEPER_FAULSE_POSITIVE_RATE = 0.01;

    // clean up door keeper every 60 intervals
    public static final int DOOR_KEEPER_MAINTENANCE_FREQ = 60;

    // Increase the value will adding pressure to indexing anomaly results and our feature query
    // OpenSearch-only setting as previous the legacy default is too low (1000)
    public static final Setting<Integer> MAX_ENTITIES_PER_QUERY = Setting
        .intSetting(
            "plugins.anomaly_detection.max_entities_per_query",
            1_000_000,
            0,
            2_000_000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // save partial zero-anomaly grade results after indexing pressure reaching the limit
    // Opendistro version has similar setting. I lowered the value to make room
    // for INDEX_PRESSURE_HARD_LIMIT. I don't find a floatSetting that has both default
    // and fallback values. I want users to use the new default value 0.6 instead of 0.8.
    // So do not plan to use the value of legacy setting as fallback.
    public static final Setting<Float> INDEX_PRESSURE_SOFT_LIMIT = Setting
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
    public static final Setting<Float> INDEX_PRESSURE_HARD_LIMIT = Setting
        .floatSetting(
            "plugins.anomaly_detection.index_pressure_hard_limit",
            0.9f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // max number of primary shards of an AD index
    public static final Setting<Integer> MAX_PRIMARY_SHARDS = Setting
        .intSetting(
            "plugins.anomaly_detection.max_primary_shards",
            LegacyOpenDistroAnomalyDetectorSettings.MAX_PRIMARY_SHARDS,
            0,
            200,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // max entity value's length
    public static int MAX_ENTITY_LENGTH = 256;

    // max number of index checkpoint requests in one bulk
    public static int MAX_BULK_CHECKPOINT_SIZE = 1000;

    // number of bulk checkpoints per second
    public static double CHECKPOINT_BULK_PER_SECOND = 0.02;

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

    public static int THRESHOLD_MODEL_TRAINING_SIZE = 128;

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

    public static final int MAX_BATCH_TASK_PIECE_SIZE = 10_000;
    public static final Setting<Integer> BATCH_TASK_PIECE_SIZE = Setting
        .intSetting(
            "plugins.anomaly_detection.batch_task_piece_size",
            LegacyOpenDistroAnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE,
            1,
            MAX_BATCH_TASK_PIECE_SIZE,
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
            1,
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
    public static final Setting<Float> COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.anomaly_detection.cold_entity_queue_max_heap_percent",
            0.001f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Float> CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.anomaly_detection.checkpoint_read_queue_max_heap_percent",
            0.001f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Float> ENTITY_COLD_START_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.anomaly_detection.entity_cold_start_queue_max_heap_percent",
            0.001f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // the percentage of heap usage allowed for queues holding large requests
    // set it to 0 to disable the queue
    public static final Setting<Float> CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.anomaly_detection.checkpoint_write_queue_max_heap_percent",
            0.01f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Float> RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT = Setting
        .floatSetting(
            "plugins.anomaly_detection.result_write_queue_max_heap_percent",
            0.01f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // expected execution time per cold entity request. This setting controls
    // the speed of cold entity requests execution. The larger, the faster, and
    // the more performance impact to customers' workload.
    public static final Setting<Integer> EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_SECS = Setting
        .intSetting(
            "plugins.anomaly_detection.expected_cold_entity_execution_time_in_secs",
            3,
            0,
            3600,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    /**
     * EntityRequest has entityName (# category fields * 256, the recommended limit
     * of a keyword field length), model Id (roughly 256 bytes), and QueuedRequest
     * fields including detector Id(roughly 128 bytes), expirationEpochMs (long,
     *  8 bytes), and priority (12 bytes).
     * Plus Java object size (12 bytes), we have roughly 928 bytes per request
     * assuming we have 2 categorical fields (plan to support 2 categorical fields now).
     * We don't want the total size exceeds 0.1% of the heap.
     * We can have at most 0.1% heap / 928 = heap / 928,000.
     * For t3.small, 0.1% heap is of 1MB. The queue's size is up to
     * 10^ 6 / 928 = 1078
     */
    public static int ENTITY_REQUEST_SIZE_IN_BYTES = 928;

    /**
     * EntityFeatureRequest consists of EntityRequest (928 bytes, read comments
     * of ENTITY_COLD_START_QUEUE_SIZE_CONSTANT), pointer to current feature
     * (8 bytes), and dataStartTimeMillis (8 bytes).  We have roughly
     * 928 + 16 = 944 bytes per request.
     *
     * We don't want the total size exceeds 0.1% of the heap.
     * We should have at most 0.1% heap / 944 = heap / 944,000
     * For t3.small, 0.1% heap is of 1MB. The queue's size is up to
     * 10^ 6 / 944 = 1059
     */
    public static int ENTITY_FEATURE_REQUEST_SIZE_IN_BYTES = 944;

    /**
     * ResultWriteRequest consists of index request (roughly 1KB), and QueuedRequest
     * fields (148 bytes, read comments of ENTITY_REQUEST_SIZE_CONSTANT).
     * Plus Java object size (12 bytes), we have roughly 1160 bytes per request
     *
     * We don't want the total size exceeds 1% of the heap.
     * We should have at most 1% heap / 1148 = heap / 116,000
     * For t3.small, 1% heap is of 10MB. The queue's size is up to
     * 10^ 7 / 1160 = 8621
     */
    public static int RESULT_WRITE_QUEUE_SIZE_IN_BYTES = 1160;

    /**
     * CheckpointWriteRequest consists of IndexRequest (200 KB), and QueuedRequest
     * fields (148 bytes, read comments of ENTITY_REQUEST_SIZE_CONSTANT).
     * The total is roughly 200 KB per request.
     *
     * We don't want the total size exceeds 1% of the heap.
     * We should have at most 1% heap / 200KB = heap / 20,000,000
     * For t3.small, 1% heap is of 10MB. The queue's size is up to
     * 10^ 7 / 2.0 * 10^5 = 50
     */
    public static int CHECKPOINT_WRITE_QUEUE_SIZE_IN_BYTES = 200_000;

    /**
     * Max concurrent entity cold starts per node
     */
    public static final Setting<Integer> ENTITY_COLD_START_QUEUE_CONCURRENCY = Setting
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
    public static final Setting<Integer> CHECKPOINT_READ_QUEUE_CONCURRENCY = Setting
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
    public static final Setting<Integer> CHECKPOINT_WRITE_QUEUE_CONCURRENCY = Setting
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
    public static final Setting<Integer> RESULT_WRITE_QUEUE_CONCURRENCY = Setting
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
    public static final Setting<Integer> CHECKPOINT_READ_QUEUE_BATCH_SIZE = Setting
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
    public static final Setting<Integer> CHECKPOINT_WRITE_QUEUE_BATCH_SIZE = Setting
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
    public static final Setting<Integer> RESULT_WRITE_QUEUE_BATCH_SIZE = Setting
        .intSetting(
            "plugins.anomaly_detection.result_write_queue_batch_size",
            5000,
            1,
            15000,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Duration QUEUE_MAINTENANCE = Duration.ofMinutes(10);

    public static final float MAX_QUEUED_TASKS_RATIO = 0.5f;

    public static final float MEDIUM_SEGMENT_PRUNE_RATIO = 0.1f;

    public static final float LOW_SEGMENT_PRUNE_RATIO = 0.3f;

    // expensive maintenance (e.g., queue maintenance) with 1/10000 probability
    public static final int MAINTENANCE_FREQ_CONSTANT = 10000;

    // ======================================
    // Checkpoint setting
    // ======================================
    // we won't accept a checkpoint larger than 30MB. Or we risk OOM.
    // For reference, in RCF 1.0, the checkpoint of a RCF with 50 trees, 10 dimensions,
    // 256 samples is of 3.2MB.
    // In compact rcf, the same RCF is of 163KB.
    // Since we allow at most 5 features, and the default shingle size is 8 and default
    // tree number size is 100, we can have at most 25.6 MB in RCF 1.0.
    // It is possible that cx increases the max features or shingle size, but we don't want
    // to risk OOM for the flexibility.
    public static final int MAX_CHECKPOINT_BYTES = 30_000_000;

    // Sets the cap on the number of buffer that can be allocated by the rcf deserialization
    // buffer pool. Each buffer is of 512 bytes. Memory occupied by 20 buffers is 10.24 KB.
    public static final int MAX_TOTAL_RCF_SERIALIZATION_BUFFERS = 20;

    // the size of the buffer used for rcf deserialization
    public static final int SERIALIZATION_BUFFER_BYTES = 512;

    // ======================================
    // pagination setting
    // ======================================
    // pagination size
    public static final Setting<Integer> PAGE_SIZE = Setting
        .intSetting("plugins.anomaly_detection.page_size", 1_000, 0, 10_000, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // within an interval, how many percents are used to process requests.
    // 1.0 means we use all of the detection interval to process requests.
    // to ensure we don't block next interval, it is better to set it less than 1.0.
    public static final float INTERVAL_RATIO_FOR_REQUESTS = 0.8f;

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
        .intSetting("plugins.anomaly_detection.max_concurrent_preview", 5, 1, 20, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // preview timeout in terms of milliseconds
    public static final long PREVIEW_TIMEOUT_IN_MILLIS = 60_000;
}
