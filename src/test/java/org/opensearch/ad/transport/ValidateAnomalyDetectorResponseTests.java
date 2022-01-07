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
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.model.DetectorValidationIssue;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;

public class ValidateAnomalyDetectorResponseTests extends AbstractADTest {

    @Test
    public void testResponseSerialization() throws IOException {
        Map<String, String> subIssues = new HashMap<>();
        subIssues.put("a", "b");
        subIssues.put("c", "d");
        DetectorValidationIssue issue = TestHelpers.randomDetectorValidationIssueWithSubIssues(subIssues);
        ValidateAnomalyDetectorResponse response = new ValidateAnomalyDetectorResponse(issue);
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        ValidateAnomalyDetectorResponse readResponse = ValidateAnomalyDetectorAction.INSTANCE.getResponseReader().read(streamInput);
        assertEquals("serialization has the wrong issue", issue, readResponse.getIssue());
    }

    @Test
    public void testResponseSerializationWithEmptyIssue() throws IOException {
        ValidateAnomalyDetectorResponse response = new ValidateAnomalyDetectorResponse((DetectorValidationIssue) null);
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        ValidateAnomalyDetectorResponse readResponse = ValidateAnomalyDetectorAction.INSTANCE.getResponseReader().read(streamInput);
        assertNull("serialization should have empty issue", readResponse.getIssue());
    }

    public void testResponseToXContentWithSubIssues() throws IOException {
        Map<String, String> subIssues = new HashMap<>();
        subIssues.put("a", "b");
        subIssues.put("c", "d");
        DetectorValidationIssue issue = TestHelpers.randomDetectorValidationIssueWithSubIssues(subIssues);
        ValidateAnomalyDetectorResponse response = new ValidateAnomalyDetectorResponse(issue);
        String validationResponse = TestHelpers.xContentBuilderToString(response.toXContent(TestHelpers.builder()));
        String message = issue.getMessage();
        assertEquals(
            "{\"detector\":{\"name\":{\"message\":\"" + message + "\",\"sub_issues\":{\"a\":\"b\",\"c\":\"d\"}}}}",
            validationResponse
        );
    }

    public void testResponseToXContent() throws IOException {
        DetectorValidationIssue issue = TestHelpers.randomDetectorValidationIssue();
        ValidateAnomalyDetectorResponse response = new ValidateAnomalyDetectorResponse(issue);
        String validationResponse = TestHelpers.xContentBuilderToString(response.toXContent(TestHelpers.builder()));
        String message = issue.getMessage();
        assertEquals("{\"detector\":{\"name\":{\"message\":\"" + message + "\"}}}", validationResponse);
    }

    public void testResponseToXContentNull() throws IOException {
        ValidateAnomalyDetectorResponse response = new ValidateAnomalyDetectorResponse((DetectorValidationIssue) null);
        String validationResponse = TestHelpers.xContentBuilderToString(response.toXContent(TestHelpers.builder()));
        assertEquals("{}", validationResponse);
    }
}
