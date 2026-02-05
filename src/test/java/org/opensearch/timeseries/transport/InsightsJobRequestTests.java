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

    public void testStatusRequestSerialization() throws IOException {
        InsightsJobRequest request = new InsightsJobRequest(rawPathBase + "_status");
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        InsightsJobRequest copy = new InsightsJobRequest(in);

        assertTrue(copy.isStatusOperation());
        assertEquals(rawPathBase + "_status", copy.getRawPath());
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
}
