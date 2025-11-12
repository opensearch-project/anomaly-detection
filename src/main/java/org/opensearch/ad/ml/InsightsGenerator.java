/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.ml;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.DetectorMetadata;
import org.opensearch.ad.model.MLMetricsCorrelationInput;
import org.opensearch.ad.model.MLMetricsCorrelationOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;

import com.google.gson.Gson;

/** 
 * Transforms ML output into structured insights-results documents
 */
public class InsightsGenerator {

    private static final Logger log = LogManager.getLogger(InsightsGenerator.class);
    private static final Gson gson = new Gson();

    /**
     * Generate insights document from ML Commons output.
     * 
     * @param mlOutput ML Commons correlation results
     * @param input Original input containing metadata
     * @return XContentBuilder ready to index
     */
    public static XContentBuilder generateInsights(MLMetricsCorrelationOutput mlOutput, MLMetricsCorrelationInput input)
        throws IOException {

        log.info("Generating insights from {} inference results", mlOutput.getInferenceResults().size());

        List<MLMetricsCorrelationOutput.InferenceResult> results = mlOutput.getInferenceResults();

        // Collect all unique detector IDs, indices, and series keys
        Set<String> allDetectorIds = new HashSet<>();
        Set<String> allIndices = new HashSet<>();
        Set<String> allSeriesKeys = new HashSet<>();

        // Generate paragraphs from each inference result
        List<Map<String, Object>> paragraphs = new ArrayList<>();

        for (MLMetricsCorrelationOutput.InferenceResult result : results) {
            Map<String, Object> paragraph = generateParagraph(result, input, allDetectorIds, allIndices, allSeriesKeys);
            if (paragraph != null) {
                paragraphs.add(paragraph);
            }
        }

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();

        // Task metadata
        builder.field("task_id", "task_" + ADCommonName.INSIGHTS_JOB_NAME + "_" + UUID.randomUUID().toString());
        builder.field("window_start", input.getExecutionStartTime().toEpochMilli());
        builder.field("window_end", input.getExecutionEndTime().toEpochMilli());
        builder.field("generated_at", Instant.now().toEpochMilli());

        // Denormalized fields for efficient filtering
        builder.field("doc_detector_ids", new ArrayList<>(allDetectorIds));
        builder.field("doc_indices", new ArrayList<>(allIndices));
        builder.field("doc_series_keys", new ArrayList<>(allSeriesKeys));

        // Paragraphs
        builder.startArray("paragraphs");
        for (Map<String, Object> paragraph : paragraphs) {
            builder.startObject();
            for (Map.Entry<String, Object> entry : paragraph.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }
        builder.endArray();

        // Statistics
        builder.startObject("stats");
        builder.field("num_paragraphs", paragraphs.size());
        builder.field("num_detectors", allDetectorIds.size());
        builder.field("num_indices", allIndices.size());
        builder.field("num_series", allSeriesKeys.size());
        builder.endObject();

        // Raw ML output (stored but not indexed)
        builder.startObject("mlc_raw");
        String rawJson = gson.toJson(mlOutput.getRawOutput());
        java.io.InputStream is = new java.io.ByteArrayInputStream(rawJson.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        builder.rawField("data", is, org.opensearch.common.xcontent.XContentType.JSON);
        builder.endObject();

        builder.endObject();

        log
            .info(
                "Generated insights: {} paragraphs, {} detectors, {} indices",
                paragraphs.size(),
                allDetectorIds.size(),
                allIndices.size()
            );

        return builder;
    }

    /**
     * Generate a single paragraph from an inference result.
     */
    private static Map<String, Object> generateParagraph(
        MLMetricsCorrelationOutput.InferenceResult result,
        MLMetricsCorrelationInput input,
        Set<String> allDetectorIds,
        Set<String> allIndices,
        Set<String> allSeriesKeys
    ) {
        int[] eventWindow = result.getEventWindow();
        int[] suspectedMetrics = result.getSuspectedMetrics();

        if (eventWindow.length < 2 || suspectedMetrics.length == 0) {
            log.warn("Invalid inference result: eventWindow={}, suspectedMetrics={}", eventWindow.length, suspectedMetrics.length);
            return null;
        }

        // Calculate event start/end times from bucket indices
        List<Instant> bucketTimestamps = input.getBucketTimestamps();
        int startIdx = (int) eventWindow[0];
        int endIdx = (int) eventWindow[1];

        // Ensure indices are within bounds
        if (startIdx < 0 || startIdx >= bucketTimestamps.size() || endIdx < 0 || endIdx >= bucketTimestamps.size()) {
            log.warn("Event window out of bounds: [{}, {}], bucket count: {}", startIdx, endIdx, bucketTimestamps.size());
            return null;
        }

        Instant eventStart = bucketTimestamps.get(startIdx);
        Instant eventEnd = bucketTimestamps.get(endIdx);

        // Extract detector IDs, indices, and series keys from suspected metrics
        List<String> metricKeys = input.getMetricKeys();
        Map<String, DetectorMetadata> detectorMetadataMap = input.getDetectorMetadataMap();

        Set<String> detectorIds = new HashSet<>();
        Set<String> indices = new HashSet<>();
        Set<String> seriesKeys = new HashSet<>();
        Set<String> entities = new HashSet<>();

        for (int metricIdx : suspectedMetrics) {
            if (metricIdx >= 0 && metricIdx < metricKeys.size()) {
                String metricKey = metricKeys.get(metricIdx);

                // Parse metric key: "detector_id" or "detector_id|entity_key"
                String[] parts = metricKey.split("\\|", 2);
                String detectorId = parts[0];
                detectorIds.add(detectorId);

                if (parts.length > 1) {
                    String seriesKey = parts[1];
                    seriesKeys.add(seriesKey);
                    entities.add(seriesKey);
                }

                // Get detector metadata
                DetectorMetadata metadata = detectorMetadataMap.get(detectorId);
                if (metadata != null && metadata.getIndices() != null) {
                    indices.addAll(metadata.getIndices());
                }
            }
        }

        // Generate paragraph text
        String text = generateParagraphText(detectorIds, indices, seriesKeys, eventStart, eventEnd, suspectedMetrics.length);

        // Update global sets
        allDetectorIds.addAll(detectorIds);
        allIndices.addAll(indices);
        allSeriesKeys.addAll(seriesKeys);

        // Build paragraph object
        Map<String, Object> paragraph = new HashMap<>();
        paragraph.put("start", eventStart.toString());
        paragraph.put("end", eventEnd.toString());
        paragraph.put("text", text);
        paragraph.put("detector_ids", new ArrayList<>(detectorIds));
        paragraph.put("indices", new ArrayList<>(indices));
        paragraph.put("entities", new ArrayList<>(entities));
        paragraph.put("series_keys", new ArrayList<>(seriesKeys));

        return paragraph;
    }

    /**
     * Generate user-friendly paragraph text.
     */
    private static String generateParagraphText(
        Set<String> detectorIds,
        Set<String> indices,
        Set<String> seriesKeys,
        Instant eventStart,
        Instant eventEnd,
        int numMetrics
    ) {
        StringBuilder text = new StringBuilder();

        DateTimeFormatter friendlyFormatter = DateTimeFormatter.ofPattern("MMM d, yyyy HH:mm z", Locale.ROOT).withZone(ZoneOffset.UTC);
        String startStr = friendlyFormatter.format(eventStart);
        String endStr = friendlyFormatter.format(eventEnd);

        text.append(String.format(Locale.ROOT, "Correlated anomalies detected across %d detector(s)", detectorIds.size()));

        if (!indices.isEmpty()) {
            text.append(String.format(Locale.ROOT, " in %d index pattern(s)", indices.size()));
        }

        if (!seriesKeys.isEmpty()) {
            text.append(String.format(Locale.ROOT, ", affecting %d entities", seriesKeys.size()));
        }

        text.append(".");
        text.append(String.format(Locale.ROOT, " Detected from %s to %s with %d correlated metrics.", startStr, endStr, numMetrics));

        return text.toString();
    }
}
