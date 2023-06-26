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
    public static final int MAX_SHINGLE_SIZE = 60;

    public static final String CONFIG_INDEX_MAPPING_FILE = "mappings/anomaly-detectors.json";

    public static final String JOBS_INDEX_MAPPING_FILE = "mappings/anomaly-detector-jobs.json";

    // 100,000 insertions costs roughly 1KB.
    public static final int DOOR_KEEPER_FOR_COLD_STARTER_MAX_INSERTION = 100_000;

    public static final double DOOR_KEEPER_FALSE_POSITIVE_RATE = 0.01;

    // clean up door keeper every 60 intervals
    public static final int DOOR_KEEPER_MAINTENANCE_FREQ = 60;

    // 1 million insertion costs roughly 1 MB.
    public static final int DOOR_KEEPER_FOR_CACHE_MAX_INSERTION = 1_000_000;

    // for a real-time operation, we trade off speed for memory as real time opearation
    // only has to do one update/scoring per interval
    public static final double REAL_TIME_BOUNDING_BOX_CACHE_RATIO = 0;

    // ======================================
    // Historical analysis
    // ======================================
    public static final int MAX_BATCH_TASK_PIECE_SIZE = 10_000;

    // within an interval, how many percents are used to process requests.
    // 1.0 means we use all of the detection interval to process requests.
    // to ensure we don't block next interval, it is better to set it less than 1.0.
    public static final float INTERVAL_RATIO_FOR_REQUESTS = 0.9f;

    public static final Duration HOURLY_MAINTENANCE = Duration.ofHours(1);

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
     * FeatureRequest has entityName (# category fields * 256, the recommended limit
     * of a keyword field length), model Id (roughly 256 bytes), and QueuedRequest
     * fields including config Id(roughly 128 bytes), dataStartTimeMillis (long,
     *  8 bytes), and currentFeature (16 bytes, assume two features on average).
     * Plus Java object size (12 bytes), we have roughly 932 bytes per request
     * assuming we have 2 categorical fields (plan to support 2 categorical fields now).
     * We don't want the total size exceeds 0.1% of the heap.
     * We can have at most 0.1% heap / 932 = heap / 932,000.
     * For t3.small, 0.1% heap is of 1MB. The queue's size is up to
     * 10^ 6 / 932 = 1072
     */
    public static int FEATURE_REQUEST_SIZE_IN_BYTES = 932;

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

    public static final int NUM_TREES = 30;

    public static final double TIME_DECAY = 0.0001;

    // If we have 32 + shingleSize (hopefully recent) values, RCF can get up and running. It will be noisy —
    // there is a reason that default size is 256 (+ shingle size), but it may be more useful for people to
    /// start seeing some results.
    public static final int NUM_MIN_SAMPLES = 32;

    // for a batch operation, we want all of the bounding box in-place for speed
    public static final double BATCH_BOUNDING_BOX_CACHE_RATIO = 1;

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
    // AD Index setting
    // ======================================
    public static int MAX_UPDATE_RETRY_TIMES = 10_000;
}
