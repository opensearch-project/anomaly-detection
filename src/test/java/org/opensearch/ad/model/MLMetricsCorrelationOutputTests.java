/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

import java.util.List;

import org.opensearch.test.OpenSearchTestCase;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class MLMetricsCorrelationOutputTests extends OpenSearchTestCase {

    public void testConstructorWithValidOutput() {
        String jsonString = "{\n"
            + "  \"inference_results\": [\n"
            + "    {\n"
            + "      \"event_window\": [52, 72],\n"
            + "      \"event_pattern\": [0, 0.1, 0.29541212, 0.2],\n"
            + "      \"suspected_metrics\": [0, 1, 2]\n"
            + "    }\n"
            + "  ]\n"
            + "}";

        JsonObject json = JsonParser.parseString(jsonString).getAsJsonObject();
        MLMetricsCorrelationOutput output = new MLMetricsCorrelationOutput(json);

        assertNotNull(output.getRawOutput());
        assertEquals(1, output.getInferenceResults().size());
    }

    public void testGetRawOutput() {
        JsonObject json = new JsonObject();
        json.addProperty("test", "value");

        MLMetricsCorrelationOutput output = new MLMetricsCorrelationOutput(json);

        assertEquals(json, output.getRawOutput());
        assertTrue(output.getRawOutput().has("test"));
        assertEquals("value", output.getRawOutput().get("test").getAsString());
    }

    public void testWithEmptyInferenceResults() {
        String jsonString = "{\"inference_results\": []}";
        JsonObject json = JsonParser.parseString(jsonString).getAsJsonObject();

        MLMetricsCorrelationOutput output = new MLMetricsCorrelationOutput(json);

        assertNotNull(output.getInferenceResults());
        assertTrue(output.getInferenceResults().isEmpty());
    }

    public void testWithMissingInferenceResults() {
        JsonObject json = new JsonObject();
        json.addProperty("other_field", "value");

        MLMetricsCorrelationOutput output = new MLMetricsCorrelationOutput(json);

        assertNotNull(output.getInferenceResults());
        assertTrue(output.getInferenceResults().isEmpty());
    }

    public void testWithNullJson() {
        MLMetricsCorrelationOutput output = new MLMetricsCorrelationOutput(null);

        assertNull(output.getRawOutput());
        assertNotNull(output.getInferenceResults());
        assertTrue(output.getInferenceResults().isEmpty());
    }

    public void testWithMultipleInferenceResults() {
        String jsonString = "{\n"
            + "  \"inference_results\": [\n"
            + "    {\n"
            + "      \"event_window\": [10, 20],\n"
            + "      \"event_pattern\": [0.1, 0.2],\n"
            + "      \"suspected_metrics\": [0, 1]\n"
            + "    },\n"
            + "    {\n"
            + "      \"event_window\": [50, 60],\n"
            + "      \"event_pattern\": [0.3, 0.4],\n"
            + "      \"suspected_metrics\": [2, 3]\n"
            + "    }\n"
            + "  ]\n"
            + "}";

        JsonObject json = JsonParser.parseString(jsonString).getAsJsonObject();
        MLMetricsCorrelationOutput output = new MLMetricsCorrelationOutput(json);

        assertEquals(2, output.getInferenceResults().size());
    }

    public void testInferenceResultConstructor() {
        JsonObject json = new JsonObject();

        JsonArray eventWindow = new JsonArray();
        eventWindow.add(52);
        eventWindow.add(72);
        json.add("event_window", eventWindow);

        JsonArray eventPattern = new JsonArray();
        eventPattern.add(0.0);
        eventPattern.add(0.29541212);
        eventPattern.add(0.0);
        json.add("event_pattern", eventPattern);

        JsonArray suspectedMetrics = new JsonArray();
        suspectedMetrics.add(0);
        suspectedMetrics.add(1);
        suspectedMetrics.add(2);
        json.add("suspected_metrics", suspectedMetrics);

        MLMetricsCorrelationOutput.InferenceResult result = new MLMetricsCorrelationOutput.InferenceResult(json);

        assertNotNull(result.getEventWindow());
        assertEquals(2, result.getEventWindow().length);
        assertEquals(52, result.getEventWindow()[0]);
        assertEquals(72, result.getEventWindow()[1]);

        assertNotNull(result.getEventPattern());
        assertEquals(3, result.getEventPattern().length);
        assertEquals(0.0, result.getEventPattern()[0], 0.001);
        assertEquals(0.29541212, result.getEventPattern()[1], 0.00001);

        assertNotNull(result.getSuspectedMetrics());
        assertEquals(3, result.getSuspectedMetrics().length);
        assertEquals(0, result.getSuspectedMetrics()[0]);
        assertEquals(1, result.getSuspectedMetrics()[1]);
        assertEquals(2, result.getSuspectedMetrics()[2]);
    }

    public void testInferenceResultWithNullArrays() {
        JsonObject json = new JsonObject();

        MLMetricsCorrelationOutput.InferenceResult result = new MLMetricsCorrelationOutput.InferenceResult(json);

        assertNotNull(result.getEventWindow());
        assertEquals(0, result.getEventWindow().length);

        assertNotNull(result.getEventPattern());
        assertEquals(0, result.getEventPattern().length);

        assertNotNull(result.getSuspectedMetrics());
        assertEquals(0, result.getSuspectedMetrics().length);
    }

    public void testInferenceResultWithEmptyArrays() {
        JsonObject json = new JsonObject();
        json.add("event_window", new JsonArray());
        json.add("event_pattern", new JsonArray());
        json.add("suspected_metrics", new JsonArray());

        MLMetricsCorrelationOutput.InferenceResult result = new MLMetricsCorrelationOutput.InferenceResult(json);

        assertEquals(0, result.getEventWindow().length);
        assertEquals(0, result.getEventPattern().length);
        assertEquals(0, result.getSuspectedMetrics().length);
    }

    public void testInferenceResultWithLargeEventPattern() {
        // Test with 125 buckets (real ML Commons use case)
        JsonObject json = new JsonObject();

        JsonArray eventWindow = new JsonArray();
        eventWindow.add(52);
        eventWindow.add(72);
        json.add("event_window", eventWindow);

        JsonArray eventPattern = new JsonArray();
        for (int i = 0; i < 125; i++) {
            eventPattern.add(randomDouble());
        }
        json.add("event_pattern", eventPattern);

        JsonArray suspectedMetrics = new JsonArray();
        suspectedMetrics.add(0);
        suspectedMetrics.add(1);
        suspectedMetrics.add(2);
        json.add("suspected_metrics", suspectedMetrics);

        MLMetricsCorrelationOutput.InferenceResult result = new MLMetricsCorrelationOutput.InferenceResult(json);

        assertEquals(125, result.getEventPattern().length);
        assertEquals(3, result.getSuspectedMetrics().length);
    }

    public void testParsingRealMLCommonsResponse() {
        // Use actual ML Commons response format from user's example
        String realResponse = "{\n"
            + "  \"inference_results\": [\n"
            + "    {\n"
            + "      \"event_window\": [52, 72],\n"
            + "      \"event_pattern\": [0, 0, 0, 3.99625e-05, 0.0001052875, 0.29541212, 0, 0],\n"
            + "      \"suspected_metrics\": [0, 1, 2]\n"
            + "    }\n"
            + "  ]\n"
            + "}";

        JsonObject json = JsonParser.parseString(realResponse).getAsJsonObject();
        MLMetricsCorrelationOutput output = new MLMetricsCorrelationOutput(json);

        List<MLMetricsCorrelationOutput.InferenceResult> results = output.getInferenceResults();
        assertEquals(1, results.size());

        MLMetricsCorrelationOutput.InferenceResult result = results.get(0);
        assertEquals(52, result.getEventWindow()[0]);
        assertEquals(72, result.getEventWindow()[1]);
        assertEquals(8, result.getEventPattern().length);
        assertEquals(3, result.getSuspectedMetrics().length);

        // Verify scientific notation parsing
        assertEquals(3.99625e-05, result.getEventPattern()[3], 1e-10);
        assertEquals(0.29541212, result.getEventPattern()[5], 1e-8);
    }

    public void testInferenceResultWithSingleMetric() {
        JsonObject json = new JsonObject();

        JsonArray eventWindow = new JsonArray();
        eventWindow.add(10);
        eventWindow.add(20);
        json.add("event_window", eventWindow);

        JsonArray eventPattern = new JsonArray();
        eventPattern.add(0.5);
        json.add("event_pattern", eventPattern);

        JsonArray suspectedMetrics = new JsonArray();
        suspectedMetrics.add(0);
        json.add("suspected_metrics", suspectedMetrics);

        MLMetricsCorrelationOutput.InferenceResult result = new MLMetricsCorrelationOutput.InferenceResult(json);

        assertEquals(1, result.getSuspectedMetrics().length);
        assertEquals(0, result.getSuspectedMetrics()[0]);
    }

    public void testInferenceResultWithManyMetrics() {
        JsonObject json = new JsonObject();

        JsonArray suspectedMetrics = new JsonArray();
        for (int i = 0; i < 50; i++) {
            suspectedMetrics.add(i);
        }
        json.add("suspected_metrics", suspectedMetrics);
        json.add("event_window", new JsonArray());
        json.add("event_pattern", new JsonArray());

        MLMetricsCorrelationOutput.InferenceResult result = new MLMetricsCorrelationOutput.InferenceResult(json);

        assertEquals(50, result.getSuspectedMetrics().length);
        for (int i = 0; i < 50; i++) {
            assertEquals(i, result.getSuspectedMetrics()[i]);
        }
    }
}
