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
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
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
}
