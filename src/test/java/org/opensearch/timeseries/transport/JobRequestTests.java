/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import java.io.IOException;
import java.util.Locale;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.constant.CommonName;

public class JobRequestTests extends OpenSearchTestCase {
    public void testSerializationDeserialization() throws IOException {
        String configId = "test-config-id";
        String modelId = "test-model-id";
        long startMillis = 1622548800000L; // June 1, 2021 00:00:00 GMT
        long endMillis = 1622635200000L;   // June 2, 2021 00:00:00 GMT
        double[] datapoint = new double[] { 1.0, 2.0, 3.0 };
        String taskId = "test-task-id";

        // Create the original request
        SingleStreamResultRequest originalRequest = new SingleStreamResultRequest(
            configId,
            modelId,
            startMillis,
            endMillis,
            datapoint,
            taskId
        );

        // Serialize the request to a BytesStreamOutput
        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);

        // Deserialize the request from the StreamInput
        StreamInput in = out.bytes().streamInput();
        SingleStreamResultRequest deserializedRequest = new SingleStreamResultRequest(in);

        // Assert that the deserialized request matches the original
        assertEquals(originalRequest.getConfigId(), deserializedRequest.getConfigId());
        assertEquals(originalRequest.getModelId(), deserializedRequest.getModelId());
        assertEquals(originalRequest.getStart(), deserializedRequest.getStart());
        assertEquals(originalRequest.getEnd(), deserializedRequest.getEnd());
        assertArrayEquals(originalRequest.getDataPoint(), deserializedRequest.getDataPoint(), 0.0001);
        assertEquals(originalRequest.getTaskId(), deserializedRequest.getTaskId());
    }

    public void testSerializationDeserialization_NullTaskId() throws IOException {
        String configId = "test-config-id";
        String modelId = "test-model-id";
        long startMillis = 1622548800000L;
        long endMillis = 1622635200000L;
        double[] datapoint = new double[] { 1.0, 2.0, 3.0 };
        String taskId = null;

        SingleStreamResultRequest originalRequest = new SingleStreamResultRequest(
            configId,
            modelId,
            startMillis,
            endMillis,
            datapoint,
            taskId
        );

        BytesStreamOutput out = new BytesStreamOutput();
        originalRequest.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        SingleStreamResultRequest deserializedRequest = new SingleStreamResultRequest(in);

        assertEquals(originalRequest.getConfigId(), deserializedRequest.getConfigId());
        assertEquals(originalRequest.getModelId(), deserializedRequest.getModelId());
        assertEquals(originalRequest.getStart(), deserializedRequest.getStart());
        assertEquals(originalRequest.getEnd(), deserializedRequest.getEnd());
        assertArrayEquals(originalRequest.getDataPoint(), deserializedRequest.getDataPoint(), 0.0001);
        assertNull(deserializedRequest.getTaskId());
    }

    public void testToXContent() throws IOException {
        String configId = "test-config-id";
        String modelId = "test-model-id";
        long startMillis = 1622548800000L;
        long endMillis = 1622635200000L;
        double[] datapoint = new double[] { 1.0, 2.0, 3.0 };
        String taskId = "test-task-id";

        SingleStreamResultRequest request = new SingleStreamResultRequest(configId, modelId, startMillis, endMillis, datapoint, taskId);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        request.toXContent(builder, null);
        String jsonString = builder.toString();

        String expectedJson = String
            .format(
                Locale.ROOT,
                "{\"%s\":\"%s\",\"%s\":\"%s\",\"%s\":%d,\"%s\":%d,\"%s\":[1.0,2.0,3.0],\"%s\":\"%s\"}",
                CommonName.CONFIG_ID_KEY,
                configId,
                CommonName.MODEL_ID_KEY,
                modelId,
                CommonName.START_JSON_KEY,
                startMillis,
                CommonName.END_JSON_KEY,
                endMillis,
                CommonName.VALUE_LIST_FIELD,
                CommonName.RUN_ONCE_FIELD,
                taskId
            );

        assertEquals(expectedJson, jsonString);
    }

    public void testToXContent_NullTaskId() throws IOException {
        String configId = "test-config-id";
        String modelId = "test-model-id";
        long startMillis = 1622548800000L;
        long endMillis = 1622635200000L;
        double[] datapoint = new double[] { 1.0, 2.0, 3.0 };
        String taskId = null;

        SingleStreamResultRequest request = new SingleStreamResultRequest(configId, modelId, startMillis, endMillis, datapoint, taskId);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        request.toXContent(builder, null);
        String jsonString = builder.toString();

        String expectedJson = String
            .format(
                Locale.ROOT,
                "{\"%s\":\"%s\",\"%s\":\"%s\",\"%s\":%d,\"%s\":%d,\"%s\":[1.0,2.0,3.0],\"%s\":null}",
                CommonName.CONFIG_ID_KEY,
                configId,
                CommonName.MODEL_ID_KEY,
                modelId,
                CommonName.START_JSON_KEY,
                startMillis,
                CommonName.END_JSON_KEY,
                endMillis,
                CommonName.VALUE_LIST_FIELD,
                CommonName.RUN_ONCE_FIELD
            );

        assertEquals(expectedJson, jsonString);
    }

    public void testValidate_MissingConfigId() {
        String configId = null; // Missing configId
        String modelId = "test-model-id";
        long startMillis = 1622548800000L;
        long endMillis = 1622635200000L;
        double[] datapoint = new double[] { 1.0, 2.0, 3.0 };
        String taskId = "test-task-id";

        SingleStreamResultRequest request = new SingleStreamResultRequest(configId, modelId, startMillis, endMillis, datapoint, taskId);

        ActionRequestValidationException validationException = request.validate();
        assertNotNull(validationException);
        assertTrue("actual: " + validationException.getMessage(), validationException.getMessage().contains("config ID is missing"));
    }

    public void testValidate_MissingModelId() {
        String configId = "test-config-id";
        String modelId = null; // Missing modelId
        long startMillis = 1622548800000L;
        long endMillis = 1622635200000L;
        double[] datapoint = new double[] { 1.0, 2.0, 3.0 };
        String taskId = "test-task-id";

        SingleStreamResultRequest request = new SingleStreamResultRequest(configId, modelId, startMillis, endMillis, datapoint, taskId);

        ActionRequestValidationException validationException = request.validate();
        assertNotNull(validationException);
        assertTrue("actual: " + validationException.getMessage(), validationException.getMessage().contains("model ID is missing"));
    }

    public void testValidate_InvalidTimestamps() {
        String configId = "test-config-id";
        String modelId = "test-model-id";
        long startMillis = 1622635200000L; // End time before start time
        long endMillis = 1622548800000L;
        double[] datapoint = new double[] { 1.0, 2.0, 3.0 };
        String taskId = "test-task-id";

        SingleStreamResultRequest request = new SingleStreamResultRequest(configId, modelId, startMillis, endMillis, datapoint, taskId);

        ActionRequestValidationException validationException = request.validate();
        assertNotNull(validationException);
        assertTrue("actual: " + validationException.getMessage(), validationException.getMessage().contains("timestamp is invalid"));
    }
}
