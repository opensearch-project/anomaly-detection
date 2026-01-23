/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.ad.transport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.ConfigValidationIssue;
import org.opensearch.timeseries.transport.ValidateConfigResponse;

public class ValidateAnomalyDetectorResponseTests extends AbstractTimeSeriesTest {

    @Test
    public void testResponseSerialization() throws IOException {
        Map<String, String> subIssues = new HashMap<>();
        subIssues.put("a", "b");
        subIssues.put("c", "d");
        ConfigValidationIssue issue = TestHelpers.randomDetectorValidationIssueWithSubIssues(subIssues);
        ValidateConfigResponse response = new ValidateConfigResponse(issue);
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        ValidateConfigResponse readResponse = ValidateAnomalyDetectorAction.INSTANCE.getResponseReader().read(streamInput);
        assertEquals("serialization has the wrong issue", issue, readResponse.getIssue());
    }

    @Test
    public void testResponseSerializationWithEmptyIssue() throws IOException {
        ValidateConfigResponse response = new ValidateConfigResponse((ConfigValidationIssue) null);
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        ValidateConfigResponse readResponse = ValidateAnomalyDetectorAction.INSTANCE.getResponseReader().read(streamInput);
        assertNull("serialization should have empty issue", readResponse.getIssue());
    }

    public void testResponseToXContentWithSubIssues() throws IOException {
        Map<String, String> subIssues = new HashMap<>();
        subIssues.put("a", "b");
        subIssues.put("c", "d");
        ConfigValidationIssue issue = TestHelpers.randomDetectorValidationIssueWithSubIssues(subIssues);
        ValidateConfigResponse response = new ValidateConfigResponse(issue);
        String validationResponse = TestHelpers.xContentBuilderToString(response.toXContent(TestHelpers.builder()));
        String message = issue.getMessage();
        assertEquals(
            "{\"detector\":{\"name\":{\"message\":\"" + message + "\",\"sub_issues\":{\"a\":\"b\",\"c\":\"d\"}}}}",
            validationResponse
        );
    }

    public void testResponseToXContent() throws IOException {
        ConfigValidationIssue issue = TestHelpers.randomDetectorValidationIssue();
        ValidateConfigResponse response = new ValidateConfigResponse(issue);
        String validationResponse = TestHelpers.xContentBuilderToString(response.toXContent(TestHelpers.builder()));
        String message = issue.getMessage();
        assertEquals("{\"detector\":{\"name\":{\"message\":\"" + message + "\"}}}", validationResponse);
    }

    public void testResponseToXContentNull() throws IOException {
        ValidateConfigResponse response = new ValidateConfigResponse((ConfigValidationIssue) null);
        String validationResponse = TestHelpers.xContentBuilderToString(response.toXContent(TestHelpers.builder()));
        assertEquals("{}", validationResponse);
    }

    public void testResponseToXContentWithIntervalRec() throws IOException {
        long intervalRec = 5;
        ConfigValidationIssue issue = TestHelpers.randomDetectorValidationIssueWithDetectorIntervalRec(intervalRec);
        ValidateConfigResponse response = new ValidateConfigResponse(issue);
        String validationResponse = TestHelpers.xContentBuilderToString(response.toXContent(TestHelpers.builder()));
        assertEquals(
            "{\"model\":{\"detection_interval\":{\"message\":\""
                + CommonMessages.INTERVAL_REC
                + intervalRec
                + "\",\"suggested_value\":{\"period\":{\"interval\":5,\"unit\":\"Minutes\"}}}}}",
            validationResponse
        );
    }

    @Test
    public void testResponseSerializationWithIntervalRec() throws IOException {
        long intervalRec = 5;
        ConfigValidationIssue issue = TestHelpers.randomDetectorValidationIssueWithDetectorIntervalRec(intervalRec);
        ValidateConfigResponse response = new ValidateConfigResponse(issue);
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        ValidateConfigResponse readResponse = ValidateAnomalyDetectorAction.INSTANCE.getResponseReader().read(streamInput);
        assertEquals(issue, readResponse.getIssue());
    }

    @Test
    public void testFromActionResponse_SameType() {
        ValidateConfigResponse original = new ValidateConfigResponse((ConfigValidationIssue) null);
        ValidateConfigResponse result = ValidateConfigResponse.fromActionResponse(original, writableRegistry());
        assertEquals(original, result);
    }

    @Test
    public void testFromActionResponse_ActualConversion() throws IOException {
        ConfigValidationIssue issue = TestHelpers.randomDetectorValidationIssue();
        ValidateConfigResponse original = new ValidateConfigResponse(issue);

        // Create a generic ActionResponse by serializing and deserializing
        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        // Create a generic ActionResponse wrapper
        ActionResponse genericResponse = new ActionResponse() {
            @Override
            public void writeTo(StreamOutput out) throws IOException {
                original.writeTo(out);
            }
        };

        // Now test the conversion path
        ValidateConfigResponse result = ValidateConfigResponse.fromActionResponse(genericResponse, writableRegistry());
        assertNotNull(result);
        assertNotNull(result.getIssue());
        assertEquals(issue.getMessage(), result.getIssue().getMessage());
    }

    @Test
    public void testToXContentWithoutParams() throws IOException {
        ConfigValidationIssue issue = TestHelpers.randomDetectorValidationIssue();
        ValidateConfigResponse response = new ValidateConfigResponse(issue);
        String validationResponse = TestHelpers.xContentBuilderToString(response.toXContent(TestHelpers.builder()));
        String message = issue.getMessage();
        assertEquals("{\"detector\":{\"name\":{\"message\":\"" + message + "\"}}}", validationResponse);
    }

    @Test
    public void testFromActionResponse_WithSubIssues() throws IOException {
        Map<String, String> subIssues = new HashMap<>();
        subIssues.put("field1", "issue1");
        subIssues.put("field2", "issue2");
        ConfigValidationIssue issue = TestHelpers.randomDetectorValidationIssueWithSubIssues(subIssues);
        ValidateConfigResponse original = new ValidateConfigResponse(issue);

        // Test conversion through fromActionResponse
        ValidateConfigResponse result = ValidateConfigResponse.fromActionResponse(original, writableRegistry());
        assertNotNull(result);
        assertNotNull(result.getIssue());
        assertEquals(issue.getMessage(), result.getIssue().getMessage());
        assertEquals(2, result.getIssue().getSubIssues().size());
    }

    @Test
    public void testFromActionResponse_WithIOException() {
        ActionResponse badResponse = new ActionResponse() {
            @Override
            public void writeTo(StreamOutput out) throws IOException {
                throw new IOException("Simulated write failure");
            }
        };
        expectThrows(UncheckedIOException.class, () -> ValidateConfigResponse.fromActionResponse(badResponse, writableRegistry()));
    }
}
