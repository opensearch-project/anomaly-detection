/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.test.OpenSearchTestCase;

public class MLMetricsCorrelationInputTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, 2.0, 3.0), Arrays.asList(4.0, 5.0, 6.0));
        List<String> metricKeys = Arrays.asList("detector-1", "detector-2");
        Map<String, DetectorMetadata> detectorMetadataMap = new HashMap<>();
        detectorMetadataMap.put("detector-1", new DetectorMetadata("detector-1", "Detector 1", Arrays.asList("index-1")));

        Instant executionStartTime = Instant.parse("2025-01-01T00:00:00Z");
        Instant executionEndTime = Instant.parse("2025-01-01T01:00:00Z");
        long bucketSizeMillis = 60000L;
        List<Instant> bucketTimestamps = Arrays
            .asList(
                executionStartTime,
                executionStartTime.plusMillis(bucketSizeMillis),
                executionStartTime.plusMillis(2 * bucketSizeMillis)
            );

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            metricKeys,
            detectorMetadataMap,
            executionStartTime,
            executionEndTime,
            bucketSizeMillis,
            bucketTimestamps
        );

        assertEquals(matrix, input.getMatrix());
        assertEquals(metricKeys, input.getMetricKeys());
        assertEquals(detectorMetadataMap, input.getDetectorMetadataMap());
        assertEquals(executionStartTime, input.getExecutionStartTime());
        assertEquals(executionEndTime, input.getExecutionEndTime());
        assertEquals(bucketSizeMillis, input.getBucketSizeMillis());
        assertEquals(bucketTimestamps, input.getBucketTimestamps());
    }

    public void testGetNumMetrics() {
        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, 2.0, 3.0), Arrays.asList(4.0, 5.0, 6.0), Arrays.asList(7.0, 8.0, 9.0));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("m1", "m2", "m3"),
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        assertEquals(3, input.getNumMetrics());
    }

    public void testGetNumBuckets() {
        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("m1"),
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        assertEquals(5, input.getNumBuckets());
    }

    public void testGetNumBucketsWithEmptyMatrix() {
        List<List<Double>> matrix = Collections.emptyList();

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Collections.emptyList(),
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        assertEquals(0, input.getNumMetrics());
        assertEquals(0, input.getNumBuckets());
    }

    public void testWithLargeMatrix() {
        // Simulate 125 buckets x 3 metrics (real ML Commons use case)
        List<List<Double>> matrix = Arrays.asList(generateTimeSeries(125), generateTimeSeries(125), generateTimeSeries(125));

        List<String> metricKeys = Arrays.asList("detector-1", "detector-2", "detector-3|host-01");

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            metricKeys,
            Collections.emptyMap(),
            Instant.now().minusSeconds(7500), // 125 minutes ago
            Instant.now(),
            60000L, // 1 minute buckets
            Collections.emptyList()
        );

        assertEquals(3, input.getNumMetrics());
        assertEquals(125, input.getNumBuckets());
    }

    public void testWithEmptyDetectorMetadataMap() {
        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, 2.0));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("m1"),
            Collections.emptyMap(), // Empty metadata map
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        assertNotNull(input.getDetectorMetadataMap());
        assertTrue(input.getDetectorMetadataMap().isEmpty());
    }

    public void testWithMultipleDetectorMetadata() {
        Map<String, DetectorMetadata> metadataMap = new HashMap<>();
        metadataMap.put("detector-1", new DetectorMetadata("detector-1", "CPU Detector", Arrays.asList("metrics-*")));
        metadataMap.put("detector-2", new DetectorMetadata("detector-2", "Memory Detector", Arrays.asList("metrics-*")));
        metadataMap.put("detector-3", new DetectorMetadata("detector-3", "Network Detector", Arrays.asList("network-*")));

        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, 2.0), Arrays.asList(3.0, 4.0), Arrays.asList(5.0, 6.0));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("detector-1", "detector-2", "detector-3|host-01"),
            metadataMap,
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        assertEquals(3, input.getDetectorMetadataMap().size());
        assertTrue(input.getDetectorMetadataMap().containsKey("detector-1"));
        assertTrue(input.getDetectorMetadataMap().containsKey("detector-2"));
        assertTrue(input.getDetectorMetadataMap().containsKey("detector-3"));
    }

    public void testBucketTimestampsAlignment() {
        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        Instant end = Instant.parse("2025-01-01T00:03:00Z");
        long bucketSize = 60000L; // 1 minute

        List<Instant> timestamps = Arrays.asList(start, start.plusMillis(bucketSize), start.plusMillis(2 * bucketSize));

        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, 2.0, 3.0));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("m1"),
            Collections.emptyMap(),
            start,
            end,
            bucketSize,
            timestamps
        );

        assertEquals(3, input.getBucketTimestamps().size());
        assertEquals(start, input.getBucketTimestamps().get(0));
        assertEquals(start.plusMillis(bucketSize), input.getBucketTimestamps().get(1));
        assertEquals(start.plusMillis(2 * bucketSize), input.getBucketTimestamps().get(2));
    }

    // Helper method to generate time series data
    private List<Double> generateTimeSeries(int length) {
        List<Double> series = new java.util.ArrayList<>();
        for (int i = 0; i < length; i++) {
            series.add(Math.sin(i * 0.1) * 10 + randomDouble() * 2);
        }
        return series;
    }
}
