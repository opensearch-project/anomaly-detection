/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.settings;

public class TimeSeriesSettings {

    // ======================================
    // Model parameters
    // ======================================
    public static final int DEFAULT_SHINGLE_SIZE = 8;

    // max shingle size we have seen from external users
    // the larger shingle size, the harder to fill in a complete shingle
    public static final int MAX_SHINGLE_SIZE = 60;

    public static final String ANOMALY_DETECTORS_INDEX_MAPPING_FILE = "mappings/anomaly-detectors.json";

    public static final String ANOMALY_DETECTOR_JOBS_INDEX_MAPPING_FILE = "mappings/anomaly-detector-jobs.json";

    // 100,000 insertions costs roughly 1KB.
    public static final int DOOR_KEEPER_FOR_COLD_STARTER_MAX_INSERTION = 100_000;

    public static final double DOOR_KEEPER_FAULSE_POSITIVE_RATE = 0.01;

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

}
