/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.ml;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.ad.model.DetectorMetadata;
import org.opensearch.ad.model.MLMetricsCorrelationInput;
import org.opensearch.ad.model.MLMetricsCorrelationOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class InsightsGeneratorTests extends OpenSearchTestCase {

    public void testGenerateInsightsWithEmptyResults() throws IOException {
        JsonObject emptyJson = new JsonObject();
        emptyJson.add("inference_results", new JsonArray());
        MLMetricsCorrelationOutput mlOutput = new MLMetricsCorrelationOutput(emptyJson);

        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        Instant end = Instant.parse("2025-01-01T01:00:00Z");

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(),
            start,
            end,
            60000L,
            Collections.emptyList()
        );

        XContentBuilder builder = InsightsGenerator.generateInsights(mlOutput, input);
        assertNotNull(builder);

        String result = builder.toString();
        assertTrue(result.contains("task_id"));
        assertTrue(result.contains("window_start"));
        assertTrue(result.contains("window_end"));
        assertTrue(result.contains("generated_at"));
        assertTrue(result.contains("paragraphs"));
        assertTrue(result.contains("stats"));
        assertTrue(result.contains("num_paragraphs"));
        assertTrue(result.contains("mlc_raw"));
    }

    public void testGenerateInsightsWithSingleInferenceResult() throws IOException {
        JsonObject json = new JsonObject();
        JsonArray inferenceResults = new JsonArray();

        JsonObject result1 = new JsonObject();

        JsonArray eventWindow = new JsonArray();
        eventWindow.add(0);
        eventWindow.add(2);
        result1.add("event_window", eventWindow);

        JsonArray eventPattern = new JsonArray();
        eventPattern.add(0.1);
        eventPattern.add(0.5);
        eventPattern.add(0.2);
        result1.add("event_pattern", eventPattern);

        JsonArray suspectedMetrics = new JsonArray();
        suspectedMetrics.add(0);
        suspectedMetrics.add(1);
        result1.add("suspected_metrics", suspectedMetrics);

        inferenceResults.add(result1);
        json.add("inference_results", inferenceResults);

        MLMetricsCorrelationOutput mlOutput = new MLMetricsCorrelationOutput(json);

        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        Instant end = start.plusSeconds(180);

        List<Instant> timestamps = Arrays.asList(start, start.plusSeconds(60), start.plusSeconds(120));

        Map<String, DetectorMetadata> metadata = new HashMap<>();
        metadata.put("detector-1", new DetectorMetadata("detector-1", "CPU Detector", Arrays.asList("metrics-*")));
        metadata.put("detector-2", new DetectorMetadata("detector-2", "Memory Detector", Arrays.asList("metrics-*")));

        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, 2.0, 3.0), Arrays.asList(4.0, 5.0, 6.0));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("detector-1", "detector-2"),
            metadata,
            start,
            end,
            60000L,
            timestamps
        );

        XContentBuilder builder = InsightsGenerator.generateInsights(mlOutput, input);
        assertNotNull(builder);

        String result = builder.toString();
        assertTrue(result.contains("\"num_paragraphs\":1"));
        assertTrue(result.contains("\"num_detectors\":2"));
        assertTrue(result.contains("\"num_indices\":1")); // Both detectors use same index pattern
    }

    public void testGenerateInsightsWithMultiEntityDetector() throws IOException {
        JsonObject json = new JsonObject();
        JsonArray inferenceResults = new JsonArray();

        JsonObject result1 = new JsonObject();

        JsonArray eventWindow = new JsonArray();
        eventWindow.add(0);
        eventWindow.add(1);
        result1.add("event_window", eventWindow);

        JsonArray eventPattern = new JsonArray();
        eventPattern.add(0.8);
        eventPattern.add(0.9);
        result1.add("event_pattern", eventPattern);

        JsonArray suspectedMetrics = new JsonArray();
        suspectedMetrics.add(0);
        suspectedMetrics.add(1);
        result1.add("suspected_metrics", suspectedMetrics);

        inferenceResults.add(result1);
        json.add("inference_results", inferenceResults);

        MLMetricsCorrelationOutput mlOutput = new MLMetricsCorrelationOutput(json);

        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        List<Instant> timestamps = Arrays.asList(start, start.plusSeconds(60));

        Map<String, DetectorMetadata> metadata = new HashMap<>();
        metadata.put("detector-1", new DetectorMetadata("detector-1", "Single Entity", Arrays.asList("logs-*")));
        metadata.put("detector-2", new DetectorMetadata("detector-2", "Multi Entity", Arrays.asList("metrics-*")));

        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, 2.0), Arrays.asList(3.0, 4.0));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("detector-1", "detector-2|host-01"), // Second metric has entity
            metadata,
            start,
            start.plusSeconds(120),
            60000L,
            timestamps
        );

        XContentBuilder builder = InsightsGenerator.generateInsights(mlOutput, input);
        assertNotNull(builder);

        String result = builder.toString();
        assertTrue(result.contains("\"num_series\":1")); // One series key (host-01)
        assertTrue(result.contains("host-01"));
    }

    public void testGenerateInsightsWithMultipleInferenceResults() throws IOException {
        JsonObject json = new JsonObject();
        JsonArray inferenceResults = new JsonArray();

        JsonObject result1 = new JsonObject();
        JsonArray window1 = new JsonArray();
        window1.add(0);
        window1.add(1);
        result1.add("event_window", window1);
        result1.add("event_pattern", new JsonArray());
        JsonArray metrics1 = new JsonArray();
        metrics1.add(0);
        result1.add("suspected_metrics", metrics1);
        inferenceResults.add(result1);

        JsonObject result2 = new JsonObject();
        JsonArray window2 = new JsonArray();
        window2.add(2);
        window2.add(3);
        result2.add("event_window", window2);
        result2.add("event_pattern", new JsonArray());
        JsonArray metrics2 = new JsonArray();
        metrics2.add(1);
        result2.add("suspected_metrics", metrics2);
        inferenceResults.add(result2);

        json.add("inference_results", inferenceResults);
        MLMetricsCorrelationOutput mlOutput = new MLMetricsCorrelationOutput(json);

        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        List<Instant> timestamps = Arrays.asList(start, start.plusSeconds(60), start.plusSeconds(120), start.plusSeconds(180));

        Map<String, DetectorMetadata> metadata = new HashMap<>();
        metadata.put("detector-1", new DetectorMetadata("detector-1", "D1", Arrays.asList("index-1")));
        metadata.put("detector-2", new DetectorMetadata("detector-2", "D2", Arrays.asList("index-2")));

        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, 2.0, 3.0, 4.0), Arrays.asList(5.0, 6.0, 7.0, 8.0));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("detector-1", "detector-2"),
            metadata,
            start,
            start.plusSeconds(240),
            60000L,
            timestamps
        );

        XContentBuilder builder = InsightsGenerator.generateInsights(mlOutput, input);
        assertNotNull(builder);

        String result = builder.toString();
        assertTrue(result.contains("\"num_paragraphs\":2"));
    }

    public void testGenerateInsightsWithInvalidEventWindow() throws IOException {
        JsonObject json = new JsonObject();
        JsonArray inferenceResults = new JsonArray();

        JsonObject result1 = new JsonObject();
        JsonArray eventWindow = new JsonArray();
        eventWindow.add(10);
        eventWindow.add(20);
        result1.add("event_window", eventWindow);

        JsonArray eventPattern = new JsonArray();
        result1.add("event_pattern", eventPattern);

        JsonArray suspectedMetrics = new JsonArray();
        suspectedMetrics.add(0);
        result1.add("suspected_metrics", suspectedMetrics);

        inferenceResults.add(result1);
        json.add("inference_results", inferenceResults);

        MLMetricsCorrelationOutput mlOutput = new MLMetricsCorrelationOutput(json);

        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        List<Instant> timestamps = Arrays.asList(start, start.plusSeconds(60));

        Map<String, DetectorMetadata> metadata = new HashMap<>();
        metadata.put("detector-1", new DetectorMetadata("detector-1", "D1", Arrays.asList("index-1")));

        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, 2.0));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("detector-1"),
            metadata,
            start,
            start.plusSeconds(120),
            60000L,
            timestamps
        );

        XContentBuilder builder = InsightsGenerator.generateInsights(mlOutput, input);
        assertNotNull(builder);

        String result = builder.toString();
        assertTrue(result.contains("\"num_paragraphs\":0")); // Invalid event window should be skipped
    }

    public void testGenerateInsightsWithEmptySuspectedMetrics() throws IOException {
        JsonObject json = new JsonObject();
        JsonArray inferenceResults = new JsonArray();

        JsonObject result1 = new JsonObject();
        JsonArray eventWindow = new JsonArray();
        eventWindow.add(0);
        eventWindow.add(1);
        result1.add("event_window", eventWindow);
        result1.add("event_pattern", new JsonArray());
        result1.add("suspected_metrics", new JsonArray()); // Empty

        inferenceResults.add(result1);
        json.add("inference_results", inferenceResults);

        MLMetricsCorrelationOutput mlOutput = new MLMetricsCorrelationOutput(json);

        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        List<Instant> timestamps = Arrays.asList(start, start.plusSeconds(60));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            Arrays.asList(Arrays.asList(1.0, 2.0)),
            Arrays.asList("detector-1"),
            Collections.emptyMap(),
            start,
            start.plusSeconds(120),
            60000L,
            timestamps
        );

        XContentBuilder builder = InsightsGenerator.generateInsights(mlOutput, input);
        assertNotNull(builder);

        String result = builder.toString();
        assertTrue(result.contains("\"num_paragraphs\":0"));
    }

    public void testGenerateInsightsWithMissingDetectorMetadata() throws IOException {
        // Create valid ML output
        JsonObject json = new JsonObject();
        JsonArray inferenceResults = new JsonArray();

        JsonObject result1 = new JsonObject();
        JsonArray eventWindow = new JsonArray();
        eventWindow.add(0);
        eventWindow.add(1);
        result1.add("event_window", eventWindow);
        result1.add("event_pattern", new JsonArray());
        JsonArray metrics = new JsonArray();
        metrics.add(0);
        result1.add("suspected_metrics", metrics);

        inferenceResults.add(result1);
        json.add("inference_results", inferenceResults);

        MLMetricsCorrelationOutput mlOutput = new MLMetricsCorrelationOutput(json);

        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        List<Instant> timestamps = Arrays.asList(start, start.plusSeconds(60));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            Arrays.asList(Arrays.asList(1.0, 2.0)),
            Arrays.asList("detector-1"),
            Collections.emptyMap(), // No metadata
            start,
            start.plusSeconds(120),
            60000L,
            timestamps
        );

        XContentBuilder builder = InsightsGenerator.generateInsights(mlOutput, input);
        assertNotNull(builder);

        String result = builder.toString();
        assertTrue(result.contains("\"num_paragraphs\":1"));
        assertTrue(result.contains("\"num_detectors\":1"));
        assertTrue(result.contains("\"num_indices\":0")); // No indices without metadata
    }

    public void testGenerateInsightsWithRealMLCommonsFormat() throws IOException {
        String realResponse = "{\n"
            + "  \"inference_results\": [\n"
            + "    {\n"
            + "      \"event_window\": [52, 72],\n"
            + "      \"event_pattern\": [0, 0, 0, 3.99625e-05, 0.0001052875, 0.29541212, 0, 0],\n"
            + "      \"suspected_metrics\": [0, 1, 2]\n"
            + "    }\n"
            + "  ]\n"
            + "}";

        com.google.gson.JsonParser parser = new com.google.gson.JsonParser();
        JsonObject json = parser.parse(realResponse).getAsJsonObject();
        MLMetricsCorrelationOutput mlOutput = new MLMetricsCorrelationOutput(json);

        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        List<Instant> timestamps = new java.util.ArrayList<>();
        List<List<Double>> matrix = new java.util.ArrayList<>();

        for (int i = 0; i < 125; i++) {
            timestamps.add(start.plusSeconds(i * 60));
        }

        for (int m = 0; m < 3; m++) {
            List<Double> series = new java.util.ArrayList<>();
            for (int i = 0; i < 125; i++) {
                series.add(Math.sin(i * 0.1 + m) * 10);
            }
            matrix.add(series);
        }

        Map<String, DetectorMetadata> metadata = new HashMap<>();
        metadata.put("detector-1", new DetectorMetadata("detector-1", "CPU", Arrays.asList("server-metrics-*", "host-logs-*")));
        metadata.put("detector-2", new DetectorMetadata("detector-2", "Memory", Arrays.asList("server-metrics-*")));
        metadata.put("detector-3", new DetectorMetadata("detector-3", "Network", Arrays.asList("app-logs-*")));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("detector-1", "detector-2", "detector-3|host-01"),
            metadata,
            start,
            start.plusSeconds(125 * 60),
            60000L,
            timestamps
        );

        // Generate insights
        XContentBuilder builder = InsightsGenerator.generateInsights(mlOutput, input);
        assertNotNull(builder);

        String result = builder.toString();
        assertTrue(result.contains("\"num_paragraphs\":1"));
        assertTrue(result.contains("\"num_detectors\":3"));
        assertTrue(result.contains("\"num_indices\":3"));
        assertTrue(result.contains("\"num_series\":1"));
        assertTrue(result.contains("Correlated anomalies detected across 3 detector(s)"));
        assertTrue(result.contains("with 3 correlated metrics"));
    }
}
