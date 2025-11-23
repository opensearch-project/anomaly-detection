/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Output from ML Commons metrics correlation algorithm.
 * 
 * ML Commons Format:
 * {
 *   "inference_results": [
 *     {
 *       "mCorrModelTensors": [
 *         {
 *           "event_window": [start_index, end_index],  // as floats
 *           "event_pattern": [intensity_scores...],
 *           "suspected_metrics": [metric_indices...]  // as floats or ints
 *         }
 *       ]
 *     }
 *   ]
 * }
 * 
 * Each item in mCorrModelTensors represents one correlated event cluster.
 */
public class MLMetricsCorrelationOutput {

    private static final Logger log = LogManager.getLogger(MLMetricsCorrelationOutput.class);

    private final JsonObject rawOutput;
    private final List<InferenceResult> inferenceResults;

    public MLMetricsCorrelationOutput(JsonObject rawOutput) {
        this.rawOutput = rawOutput;
        this.inferenceResults = parseInferenceResults(rawOutput);
    }

    public JsonObject getRawOutput() {
        return rawOutput;
    }

    public List<InferenceResult> getInferenceResults() {
        return inferenceResults;
    }

    private List<InferenceResult> parseInferenceResults(JsonObject json) {
        List<InferenceResult> results = new ArrayList<>();

        if (json == null || !json.has("inference_results")) {
            log.warn("No 'inference_results' field found in JSON");
            return results;
        }

        JsonArray inferenceArray = json.getAsJsonArray("inference_results");

        // ML Commons returns nested structure: inference_results[].mCorrModelTensors[]
        for (int i = 0; i < inferenceArray.size(); i++) {
            JsonElement element = inferenceArray.get(i);
            JsonObject resultObj = element.getAsJsonObject();

            // Check for mCorrModelTensors array (ML Commons format)
            if (resultObj.has("mCorrModelTensors")) {
                JsonArray tensorsArray = resultObj.getAsJsonArray("mCorrModelTensors");
                log.info("Parsing {} event clusters from mCorrModelTensors", tensorsArray.size());

                for (int j = 0; j < tensorsArray.size(); j++) {
                    JsonObject tensorObj = tensorsArray.get(j).getAsJsonObject();
                    results.add(new InferenceResult(tensorObj));
                }
            } else {
                // Fallback: direct format (for backward compatibility)
                log.debug("Using direct format (no mCorrModelTensors wrapper)");
                results.add(new InferenceResult(resultObj));
            }
        }

        return results;
    }

    /**
     * Single inference result representing one correlated event.
     */
    public static class InferenceResult {
        private final int[] eventWindow;
        private final double[] eventPattern;
        private final int[] suspectedMetrics;

        public InferenceResult(JsonObject json) {
            this.eventWindow = parseIntArray(json.getAsJsonArray("event_window"));
            this.eventPattern = parseDoubleArray(json.getAsJsonArray("event_pattern"));
            this.suspectedMetrics = parseIntArray(json.getAsJsonArray("suspected_metrics"));

            log
                .debug(
                    "Parsed InferenceResult: eventWindow=[{}, {}], suspectedMetrics={} metrics, eventPattern={} values",
                    eventWindow.length >= 2 ? eventWindow[0] : "?",
                    eventWindow.length >= 2 ? eventWindow[1] : "?",
                    suspectedMetrics.length,
                    eventPattern.length
                );
        }

        public int[] getEventWindow() {
            return eventWindow;
        }

        public double[] getEventPattern() {
            return eventPattern;
        }

        public int[] getSuspectedMetrics() {
            return suspectedMetrics;
        }

        private int[] parseIntArray(JsonArray array) {
            if (array == null) {
                return new int[0];
            }
            int[] result = new int[array.size()];
            for (int i = 0; i < array.size(); i++) {
                // ML Commons may return floats (e.g., 16.0) for integer values
                JsonElement element = array.get(i);
                if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isNumber()) {
                    result[i] = element.getAsNumber().intValue();
                } else {
                    result[i] = element.getAsInt();
                }
            }
            return result;
        }

        private double[] parseDoubleArray(JsonArray array) {
            if (array == null) {
                return new double[0];
            }
            double[] result = new double[array.size()];
            for (int i = 0; i < array.size(); i++) {
                result[i] = array.get(i).getAsDouble();
            }
            return result;
        }
    }
}
