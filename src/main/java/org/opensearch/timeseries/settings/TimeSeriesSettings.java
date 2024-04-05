/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.settings;

import java.time.Duration;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

public class TimeSeriesSettings {

    // ======================================
    // Model parameters
    // ======================================
    public static final int DEFAULT_SHINGLE_SIZE = 8;

    // max shingle size we have seen from external users
    // the larger shingle size, the harder to fill in a complete shingle
    public static final int MAX_SHINGLE_SIZE = 64;

    // shingle size = seasonality / 2
    public static final int SEASONALITY_TO_SHINGLE_RATIO = 2;

    public static final String CONFIG_INDEX_MAPPING_FILE = "mappings/config.json";

    public static final String JOBS_INDEX_MAPPING_FILE = "mappings/job.json";

    /**
     * Memory Usage Estimation for a Map&lt;String, Integer&gt; with 100,000 entries:
     *
     * 1. HashMap Object Overhead: This can vary, but let's assume it's about 36 bytes.
     * 2. Array Overhead:
     *    - The array size will be the nearest power of 2 greater than or equal to 100,000 / load factor.
     *    - Assuming a load factor of 0.75, the array size will be 2^17 = 131,072.
     *    - The memory usage will be 131,072 * 4 bytes = 524,288 bytes.
     * 3. Entry Overhead: Each entry has an overhead of about 32 bytes (object header, hash code, and three references).
     * 4. Key Overhead:
     *    - Each key has an overhead of about 36 bytes (object header, length, hash cache) plus the character data.
     *    - Assuming the character data is 64 bytes, the total key overhead per entry is 100 bytes.
     * 5. Value Overhead: Each Integer object has an overhead of about 16 bytes (object header plus int value).
     *
     * Total Memory Usage Formula:
     * Total Memory Usage = HashMap Object Overhead + Array Overhead +
     *                      (Entry Overhead + Key Overhead + Value Overhead) * Number of Entries
     *
     * Plugging in the numbers:
     * Total Memory Usage = 36 + 524,288 + (32 + 100 + 16) * 100,000
     *                    ≈ 14,965 kilobytes (≈ 15 MB)
     *
     * Note:
     * This estimation is quite simplistic and the actual memory usage may be different based on the JVM implementation,
     * the actual Map implementation being used, and other factors.
     */
    public static final int DOOR_KEEPER_FOR_COLD_STARTER_MAX_INSERTION = 100_000;

    // clean up door keeper every 60 intervals
    public static final int DOOR_KEEPER_MAINTENANCE_FREQ = 60;

    // 1 million insertion costs roughly 1 MB.
    public static final int DOOR_KEEPER_FOR_CACHE_MAX_INSERTION = 1_000_000;

    // for a real-time operation, we trade off speed for memory as real time opearation
    // only has to do one update/scoring per interval
    public static final double REAL_TIME_BOUNDING_BOX_CACHE_RATIO = 0;

    // max number of historical buckets for cold start. Corresponds to max buckets in OpenSearch.
    // We send one query including one bucket per interval. So we don't want to surpass OS limit.
    public static final int MAX_HISTORY_INTERVALS = 10000;

    // ======================================
    // Historical analysis
    // ======================================
    public static final int MAX_BATCH_TASK_PIECE_SIZE = 10_000;

    // within an interval, how many percents are used to process requests.
    // 1.0 means we use all of the detection interval to process requests.
    // to ensure we don't block next interval, it is better to set it less than 1.0.
    public static final float INTERVAL_RATIO_FOR_REQUESTS = 0.9f;

    public static final Duration HOURLY_MAINTENANCE = Duration.ofHours(1);

    // Maximum number of deleted tasks can keep in cache.
    public static final Setting<Integer> MAX_CACHED_DELETED_TASKS = Setting
        .intSetting("plugins.timeseries.max_cached_deleted_tasks", 1000, 1, 10_000, Setting.Property.NodeScope, Setting.Property.Dynamic);

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
    // rate-limiting queue parameters
    // ======================================
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
     * ADResultWriteRequest consists of index request (roughly 1KB), and QueuedRequest
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
     * FeatureRequest has entity (max 2 category fields * 256, the recommended limit
     * of a keyword field length, 512 bytes), model Id (roughly 256 bytes), runOnce
     * boolean (roughly 8 bytes), dataStartTimeMillis (long,
     *  8 bytes), and currentFeature (16 bytes, assume two features on average).
     * Plus Java object size (12 bytes), we have roughly 812 bytes per request
     * assuming we have 2 categorical fields (plan to support 2 categorical fields now).
     * We don't want the total size exceeds 0.1% of the heap.
     * We can have at most 0.1% heap / 812 = heap / 812,000.
     * For t3.small, 0.1% heap is of 1MB. The queue's size is up to
     * 10^ 6 / 812 = 1231
     */
    public static int FEATURE_REQUEST_SIZE_IN_BYTES = 812;

    /**
     * CheckpointMaintainRequest has model Id (roughly 256 bytes), and QueuedRequest
     * fields including detector Id(roughly 128 bytes), expirationEpochMs (long,
     *  8 bytes), and priority (12 bytes).
     * Plus Java object size (12 bytes), we have roughly 416 bytes per request.
     * We don't want the total size exceeds 0.1% of the heap.
     * We can have at most 0.1% heap / 416 = heap / 416,000.
     * For t3.small, 0.1% heap is of 1MB. The queue's size is up to
     * 10^ 6 / 416 = 2403
     */
    public static int CHECKPOINT_MAINTAIN_REQUEST_SIZE_IN_BYTES = 416;

    public static final float MAX_QUEUED_TASKS_RATIO = 0.5f;

    public static final float MEDIUM_SEGMENT_PRUNE_RATIO = 0.1f;

    public static final float LOW_SEGMENT_PRUNE_RATIO = 0.3f;

    // expensive maintenance (e.g., queue maintenance) with 1/10000 probability
    public static final int MAINTENANCE_FREQ_CONSTANT = 10000;

    public static final Duration QUEUE_MAINTENANCE = Duration.ofMinutes(10);

    // ======================================
    // ML parameters
    // ======================================
    // RCF
    public static final int NUM_SAMPLES_PER_TREE = 256;

    public static final int NUM_TREES = 50;

    public static final int DEFAULT_RECENCY_EMPHASIS = 10 * NUM_SAMPLES_PER_TREE;

    // If we have 32 + shingleSize (hopefully recent) values, RCF can get up and running. It will be noisy —
    // there is a reason that default size is 256 (+ shingle size), but it may be more useful for people to
    /// start seeing some results.
    public static final int NUM_MIN_SAMPLES = 32;

    // for a batch operation, we want all of the bounding box in-place for speed
    public static final double BATCH_BOUNDING_BOX_CACHE_RATIO = 1;

    // feature processing
    public static final int TRAIN_SAMPLE_TIME_RANGE_IN_HOURS = 24;

    public static final int MIN_TRAIN_SAMPLES = 512;

    // ======================================
    // Cold start setting
    // ======================================
    public static int MAX_COLD_START_ROUNDS = 2;

    // Thresholding
    public static final double THRESHOLD_MIN_PVALUE = 0.995;

    // ======================================
    // Cold start setting
    // ======================================
    public static final Setting<Integer> MAX_RETRY_FOR_UNRESPONSIVE_NODE = Setting
        .intSetting("plugins.timeseries.max_retry_for_unresponsive_node", 5, 0, Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<TimeValue> BACKOFF_MINUTES = Setting
        .positiveTimeSetting(
            "plugins.timeseries.backoff_minutes",
            TimeValue.timeValueMinutes(15),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> COOLDOWN_MINUTES = Setting
        .positiveTimeSetting(
            "plugins.timeseries.cooldown_minutes",
            TimeValue.timeValueMinutes(5),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // ======================================
    // Index setting
    // ======================================
    public static int MAX_UPDATE_RETRY_TIMES = 10_000;

    // ======================================
    // JOB
    // ======================================
    public static final long DEFAULT_JOB_LOC_DURATION_SECONDS = 60;

    // ======================================
    // stats/profile API setting
    // ======================================
    // profile API needs to report total entities. We can use cardinality aggregation for a single-category field.
    // But we cannot do that for multi-category fields as it requires scripting to generate run time fields,
    // which is expensive. We work around the problem by using a composite query to find the first 10_000 buckets.
    // Generally, traversing all buckets/combinations can't be done without visiting all matches, which is costly
    // for data with many entities. Given that it is often enough to have a lower bound of the number of entities,
    // such as "there are at least 10000 entities", the default is set to 10,000. That is, requests will count the
    // total entities up to 10,000.
    public static final int MAX_TOTAL_ENTITIES_TO_TRACK = 10_000;

    // ======================================
    // Validate Detector API setting
    // ======================================
    public static final long TOP_VALIDATE_TIMEOUT_IN_MILLIS = 10_000;

    public static final double INTERVAL_BUCKET_MINIMUM_SUCCESS_RATE = 0.75;

    public static final double INTERVAL_RECOMMENDATION_INCREASING_MULTIPLIER = 1.2;

    public static final long MAX_INTERVAL_REC_LENGTH_IN_MINUTES = 60L;

    public static final int MAX_DESCRIPTION_LENGTH = 1000;

    // ======================================
    // Cache setting
    // ======================================
    // We don't want to retry cold start once it exceeds the threshold.
    // It is larger than 1 since cx may have ingested new data or the
    // system is unstable
    public static final int COLD_START_DOOR_KEEPER_COUNT_THRESHOLD = 3;

    // we don't admit model to cache before it exceeds the threshold
    public static final int CACHE_DOOR_KEEPER_COUNT_THRESHOLD = 1;

    // max entities to track per detector
    public static final int MAX_TRACKING_ENTITIES = 1000000;

    public static final double DOOR_KEEPER_FALSE_POSITIVE_RATE = 0.01;

    public static final double TIME_DECAY = 0.0001;
}
