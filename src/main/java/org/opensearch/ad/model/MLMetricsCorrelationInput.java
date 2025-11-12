/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Input for ML Commons metrics correlation algorithm.
 * Contains M x T matrix where M = number of metrics, T = number of time buckets.
 */
public class MLMetricsCorrelationInput {
    // M x T matrix
    private final List<List<Double>> matrix;
    // Metric: detector_id|entity_key
    private final List<String> metricKeys;
    private final Map<String, DetectorMetadata> detectorMetadataMap;
    private final Instant executionStartTime;
    private final Instant executionEndTime;
    private final long bucketSizeMillis;
    private final List<Instant> bucketTimestamps;

    public MLMetricsCorrelationInput(
        List<List<Double>> matrix,
        List<String> metricKeys,
        Map<String, DetectorMetadata> detectorMetadataMap,
        Instant executionStartTime,
        Instant executionEndTime,
        long bucketSizeMillis,
        List<Instant> bucketTimestamps
    ) {
        this.matrix = matrix;
        this.metricKeys = metricKeys;
        this.detectorMetadataMap = detectorMetadataMap;
        this.executionStartTime = executionStartTime;
        this.executionEndTime = executionEndTime;
        this.bucketSizeMillis = bucketSizeMillis;
        this.bucketTimestamps = bucketTimestamps;
    }

    public List<List<Double>> getMatrix() {
        return matrix;
    }

    public List<String> getMetricKeys() {
        return metricKeys;
    }

    public Map<String, DetectorMetadata> getDetectorMetadataMap() {
        return detectorMetadataMap;
    }

    public Instant getExecutionStartTime() {
        return executionStartTime;
    }

    public Instant getExecutionEndTime() {
        return executionEndTime;
    }

    public long getBucketSizeMillis() {
        return bucketSizeMillis;
    }

    public List<Instant> getBucketTimestamps() {
        return bucketTimestamps;
    }

    public int getNumMetrics() {
        return matrix.size();
    }

    public int getNumBuckets() {
        return matrix.isEmpty() ? 0 : matrix.get(0).size();
    }
}
