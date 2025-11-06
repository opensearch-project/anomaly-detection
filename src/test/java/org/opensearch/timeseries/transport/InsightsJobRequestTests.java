/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.timeseries.transport;

import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength;

import java.io.IOException;

import org.junit.Before;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class InsightsJobRequestTests extends OpenSearchTestCase {

    private String rawPathBase;

    @Before
    public void initRawPath() {
        rawPathBase = "/_plugins/_anomaly_detection/insights/" + randomAlphaOfLength(4);
    }

    public void testStartRequestSerialization() throws IOException {
        InsightsJobRequest request = new InsightsJobRequest("12h", rawPathBase + "_start");
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        InsightsJobRequest copy = new InsightsJobRequest(in);

        assertEquals("12h", copy.getFrequency());
        assertTrue(copy.isStartOperation());
        assertEquals(rawPathBase + "_start", copy.getRawPath());
    }

    public void testResultsRequestSerialization() throws IOException {
        InsightsJobRequest request = new InsightsJobRequest("detector-1", "index-a", 5, 10, rawPathBase + "_results");
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        InsightsJobRequest copy = new InsightsJobRequest(in);

        assertEquals("detector-1", copy.getDetectorId());
        assertEquals("index-a", copy.getIndex());
        assertEquals(5, copy.getFrom());
        assertEquals(10, copy.getSize());
        assertTrue(copy.isResultsOperation());
    }

    public void testStopRequestSerialization() throws IOException {
        InsightsJobRequest request = new InsightsJobRequest(rawPathBase + "_stop");
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        InsightsJobRequest copy = new InsightsJobRequest(in);

        assertTrue(copy.isStopOperation());
        assertEquals(rawPathBase + "_stop", copy.getRawPath());
    }

    public void testValidationRejectsNegativeFrom() {
        InsightsJobRequest request = new InsightsJobRequest("det", "index", -1, 10, rawPathBase + "_results");
        assertNotNull(request.validate());
    }

    public void testValidationRejectsNonPositiveSize() {
        InsightsJobRequest request = new InsightsJobRequest("det", "index", 0, 0, rawPathBase + "_results");
        assertNotNull(request.validate());
    }
}
