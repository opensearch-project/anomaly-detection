/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.ml;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectorMetadata;
import org.opensearch.ad.model.MLMetricsCorrelationInput;
import org.opensearch.timeseries.model.Entity;

/**
 * Builds ML Commons metrics correlation input from anomaly results.
 * 
 * Transforms a list of AnomalyResults into an M x T matrix where:
 * - M = number of metrics (unique detector_id + entity combinations)
 * - T = number of time buckets (all buckets in the analysis window)
 * - Each cell contains anomaly_score (0.0 if no anomaly in that bucket)
 * 
 * 
 *  How it builds the matrix:
 *  1. Group anomalies by metric (detector_id + entity_key)
 *    - single stream detector: detector_id
 *    - hc detector: detector_id|entity_key
 *  2. Create ALL buckets for the full time window (dense representation)
 *    - totalBuckets = (executionEnd - executionStart) / 60_000
 *    - allBucketList = [0 … totalBuckets-1]
 *    - bucketTimestamps[i] = executionStart + i * 60_000 ms
 *  3. Create bucket timestamp list for all buckets
 *    - bucketIndex = (anomaly.data_start_time - executionStart) / 60_000
 *    - Keep the max anomaly_score per bucket (if multiple fell into same minute).
 *    - for example: 
 *      Anomalies (simplified):
 *         dA (single stream)
            10:02:15 score=0.8
            10:05:05 score=0.4
            10:05:50 score=0.9
        Build time series (max per minute, zeros elsewhere): [0, 0, 0.8, 0, 0, 0.9, 0, 0, 0, 0]
            bucket 2 = 0.8
            bucket 5 = max(0.4, 0.9) = 0.9
            others = 0.0
 *  4. Build M x T matrix (with zeros for empty buckets)
 *    - build a matrix with the number of metrics as rows and the number of buckets as columns
 *    - each cell contains the anomaly score for the corresponding bucket and metric
 */

public class MLMetricsCorrelationInputBuilder {

    private static final Logger log = LogManager.getLogger(MLMetricsCorrelationInputBuilder.class);

    private static final long DEFAULT_BUCKET_SIZE_MILLIS = 60_000L; // 1 minute

    /**
     * Build metrics correlation input from anomaly results.
     * 
     * @param anomalies List of anomaly results
     * @param detectorMetadataMap Map of detector_id to metadata
     * @param executionStartTime Start of analysis window
     * @param executionEndTime End of analysis window
     * @return MLMetricsCorrelationInput ready for ML Commons API
     */
    public static MLMetricsCorrelationInput buildInput(
        List<AnomalyResult> anomalies,
        Map<String, DetectorMetadata> detectorMetadataMap,
        Instant executionStartTime,
        Instant executionEndTime
    ) {
        if (anomalies == null || anomalies.isEmpty()) {
            log.warn("No anomalies provided for metrics correlation");
            return new MLMetricsCorrelationInput(
                new ArrayList<>(),
                new ArrayList<>(),
                detectorMetadataMap,
                executionStartTime,
                executionEndTime,
                DEFAULT_BUCKET_SIZE_MILLIS,
                new ArrayList<>()
            );
        }

        log.info("Building metrics correlation input from {} anomalies", anomalies.size());

        // Step 1: group anomalies by metric (detector_id + entity_key)
        Map<String, List<AnomalyResult>> metricGroups = groupByMetric(anomalies);
        log.info("Grouped into {} unique metrics", metricGroups.size());

        // Step 2: Create ALL buckets for the full time window (dense representation)
        long windowDurationMillis = executionEndTime.toEpochMilli() - executionStartTime.toEpochMilli();
        int totalBuckets = (int) (windowDurationMillis / DEFAULT_BUCKET_SIZE_MILLIS);
        List<Integer> allBucketList = new ArrayList<>();
        for (int i = 0; i < totalBuckets; i++) {
            allBucketList.add(i);
        }
        log
            .info(
                "Created {} time buckets for full window ({}h at {}-min granularity)",
                totalBuckets,
                windowDurationMillis / (1000 * 60 * 60),
                DEFAULT_BUCKET_SIZE_MILLIS / (1000 * 60)
            );

        if (totalBuckets == 0) {
            log.warn("No time buckets to analyze (window duration too short)");
            return new MLMetricsCorrelationInput(
                new ArrayList<>(),
                new ArrayList<>(),
                detectorMetadataMap,
                executionStartTime,
                executionEndTime,
                DEFAULT_BUCKET_SIZE_MILLIS,
                new ArrayList<>()
            );
        }

        // Step 3: Create bucket timestamp list for all buckets
        List<Instant> bucketTimestamps = allBucketList
            .stream()
            .map(bucketIdx -> calculateBucketTimestamp(executionStartTime, bucketIdx, DEFAULT_BUCKET_SIZE_MILLIS))
            .collect(Collectors.toList());

        // Step 4: Build M x T matrix (with zeros for empty buckets)
        List<List<Double>> matrix = new ArrayList<>();
        List<String> metricKeys = new ArrayList<>(metricGroups.keySet());

        for (String metricKey : metricKeys) {
            List<AnomalyResult> metricAnomalies = metricGroups.get(metricKey);
            List<Double> timeSeries = buildTimeSeries(metricAnomalies, executionStartTime, allBucketList, DEFAULT_BUCKET_SIZE_MILLIS);
            matrix.add(timeSeries);
        }

        log.info("Built correlation matrix: {} metrics x {} time buckets", matrix.size(), matrix.isEmpty() ? 0 : matrix.get(0).size());

        return new MLMetricsCorrelationInput(
            matrix,
            metricKeys,
            detectorMetadataMap,
            executionStartTime,
            executionEndTime,
            DEFAULT_BUCKET_SIZE_MILLIS,
            bucketTimestamps
        );
    }

    private static Map<String, List<AnomalyResult>> groupByMetric(List<AnomalyResult> anomalies) {
        Map<String, List<AnomalyResult>> groups = new HashMap<>();

        for (AnomalyResult anomaly : anomalies) {
            String metricKey = getMetricKey(anomaly);
            groups.computeIfAbsent(metricKey, k -> new ArrayList<>()).add(anomaly);
        }

        return groups;
    }

    private static String getMetricKey(AnomalyResult anomaly) {
        String detectorId = anomaly.getDetectorId();
        Optional<Entity> optEntity = anomaly.getEntity();
        if (!optEntity.isPresent()) {
            // single stream detector
            return detectorId;
        }

        // hc detector
        Entity entity = optEntity.get();
        String entityKey = entity
            .getAttributes()
            .entrySet()
            .stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .sorted()
            .collect(Collectors.joining(","));

        return detectorId + "|" + entityKey;
    }

    /**
     * Build time series for a single metric.
     * Returns array of anomaly scores for each bucket (0.0 if no anomaly in that bucket).
     */
    private static List<Double> buildTimeSeries(
        List<AnomalyResult> metricAnomalies,
        Instant executionStartTime,
        List<Integer> allBucketList,
        long bucketSizeMillis
    ) {
        // Build map of bucket_index -> max_anomaly_score
        Map<Integer, Double> bucketScores = new TreeMap<>();
        long startTimeMillis = executionStartTime.toEpochMilli();

        for (AnomalyResult anomaly : metricAnomalies) {
            long anomalyTime = anomaly.getDataStartTime().toEpochMilli();
            int bucketIndex = (int) ((anomalyTime - startTimeMillis) / bucketSizeMillis);

            if (bucketIndex >= 0) {
                // Use MAX score if multiple anomalies in same bucket
                double currentScore = bucketScores.getOrDefault(bucketIndex, 0.0);
                double newScore = anomaly.getAnomalyScore();
                bucketScores.put(bucketIndex, Math.max(currentScore, newScore));
            }
        }

        // Build time series array for all buckets, with zeros for empty buckets
        List<Double> timeSeries = new ArrayList<>();
        for (Integer bucketIndex : allBucketList) {
            timeSeries.add(bucketScores.getOrDefault(bucketIndex, 0.0));
        }

        return timeSeries;
    }

    /**
     * Calculate timestamp for a bucket index.
     */
    private static Instant calculateBucketTimestamp(Instant executionStartTime, int bucketIndex, long bucketSizeMillis) {
        long bucketTimeMillis = executionStartTime.toEpochMilli() + (bucketIndex * bucketSizeMillis);
        return Instant.ofEpochMilli(bucketTimeMillis);
    }
}
