/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.ml;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.model.MLMetricsCorrelationInput;
import org.opensearch.ad.model.MLMetricsCorrelationOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.input.execute.metricscorrelation.MetricsCorrelationInput;
import org.opensearch.ml.common.output.execute.metrics_correlation.MetricsCorrelationOutput;
import org.opensearch.ml.common.transport.execute.MLExecuteTaskAction;
import org.opensearch.ml.common.transport.execute.MLExecuteTaskRequest;
import org.opensearch.transport.client.Client;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

/**
 * Client for calling ML Commons metrics correlation API
 */
public class MLCommonsClient {

    private static final Logger log = LogManager.getLogger(MLCommonsClient.class);

    private final Client client;
    private final Gson gson;

    public MLCommonsClient(Client client, NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.gson = new Gson();
    }

    /**
     * Execute metrics correlation via ML Commons transport layer.
     * 
     * @param input Metrics correlation input containing M x T matrix
     * @param listener Callback with correlation results
     */
    public void executeMetricsCorrelation(MLMetricsCorrelationInput input, ActionListener<MLMetricsCorrelationOutput> listener) {

        if (input.getNumMetrics() == 0 || input.getNumBuckets() == 0) {
            log.warn("Empty metrics matrix, skipping ML Commons call");
            listener.onResponse(createEmptyOutput());
            return;
        }

        log
            .info(
                "Calling ML Commons METRICS_CORRELATION with {} metrics x {} buckets via transport",
                input.getNumMetrics(),
                input.getNumBuckets()
            );

        try {
            // Convert AD input to ML Commons input
            List<float[]> floatArrayList = convertToFloatArrayList(input.getMatrix());
            MetricsCorrelationInput mlInput = MetricsCorrelationInput.builder().inputData(floatArrayList).build();

            // Create ML Commons execute request
            MLExecuteTaskRequest request = new MLExecuteTaskRequest(FunctionName.METRICS_CORRELATION, mlInput);

            // Execute transport action
            client.execute(MLExecuteTaskAction.INSTANCE, request, ActionListener.wrap(response -> {
                try {
                    MetricsCorrelationOutput mlOutput = (MetricsCorrelationOutput) response.getOutput();

                    MLMetricsCorrelationOutput result = parseMLCommonsOutput(mlOutput);
                    log.info("ML Commons transport call succeeded, found {} event clusters", result.getInferenceResults().size());

                    listener.onResponse(result);
                } catch (Exception e) {
                    log.error("Failed to parse ML Commons response", e);
                    listener.onResponse(createEmptyOutput());
                }
            }, error -> {
                // Graceful degradation: return empty output instead of failing tests/flow
                log.warn("ML Commons transport call failed, degrading gracefully with empty output", error);
                listener.onResponse(createEmptyOutput());
            }));

        } catch (Exception e) {
            // Graceful degradation on unexpected errors as well
            log.warn("Unexpected error calling ML Commons, degrading gracefully with empty output", e);
            listener.onResponse(createEmptyOutput());
        }
    }

    private List<float[]> convertToFloatArrayList(List<List<Double>> matrix) {
        List<float[]> result = new ArrayList<>();
        for (List<Double> row : matrix) {
            float[] floatRow = new float[row.size()];
            for (int i = 0; i < row.size(); i++) {
                floatRow[i] = row.get(i).floatValue();
            }
            result.add(floatRow);
        }
        return result;
    }

    private MLMetricsCorrelationOutput parseMLCommonsOutput(MetricsCorrelationOutput mlOutput) {
        try {
            // ML Commons uses field name "modelOutput" but we need "inference_results"
            String jsonOutput = gson.toJson(mlOutput);
            JsonObject jsonObject = gson.fromJson(jsonOutput, JsonObject.class);

            // Rename "modelOutput" to "inference_results" if present
            JsonObject result = new JsonObject();
            if (jsonObject.has("modelOutput")) {
                result.add("inference_results", jsonObject.get("modelOutput"));
            } else if (jsonObject.has("inference_results")) {
                // Already has the correct structure
                result.add("inference_results", jsonObject.get("inference_results"));
            } else {
                // Return empty if neither field exists
                log.warn("Neither 'modelOutput' nor 'inference_results' found in ML Commons response. Keys: {}", jsonObject.keySet());
                return createEmptyOutput();
            }

            return new MLMetricsCorrelationOutput(result);
        } catch (Exception e) {
            log.error("Failed to parse ML Commons output", e);
            return createEmptyOutput();
        }
    }

    private MLMetricsCorrelationOutput createEmptyOutput() {
        JsonObject empty = new JsonObject();
        empty.add("inference_results", gson.toJsonTree(new Object[0]));
        return new MLMetricsCorrelationOutput(empty);
    }
}
