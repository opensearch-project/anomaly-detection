/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

/** Unit tests for {@link ADHCImputeNodeRequest}. */
public class ADHCImputeNodeRequestTests extends OpenSearchTestCase {

    /* ------------------------------------------------------------------
       Full round-trip: writeTo -> ctor(StreamInput)
       ------------------------------------------------------------------ */
    public void testRoundTripSerialization() throws IOException {
        /* Build a representative ADHCImputeRequest.
         * Adjust field list / ctor as necessary for your code-base. */
        String detectorId = randomAlphaOfLength(10);
        String taskId = randomAlphaOfLength(8);
        long startMillis = 1L;
        long endMillis = 2L;
        ADHCImputeRequest innerReq = new ADHCImputeRequest(detectorId, taskId, startMillis, endMillis);

        ADHCImputeNodeRequest nodeReq = new ADHCImputeNodeRequest(innerReq);

        // ----- write -----
        BytesStreamOutput out = new BytesStreamOutput();
        nodeReq.writeTo(out);
        BytesReference bytes = out.bytes();

        // ----- read -----
        StreamInput in = bytes.streamInput();
        ADHCImputeNodeRequest readNodeReq = new ADHCImputeNodeRequest(in);
        ADHCImputeRequest readInner = readNodeReq.getRequest();

        // ----- assertions -----
        assertEquals("detectorId mismatch after round-trip", detectorId, readInner.getConfigId());
        assertEquals("taskId mismatch after round-trip", taskId, readInner.getTaskId());
        assertEquals("start time mismatch after round-trip", startMillis, readInner.getDataStartMillis());
        assertEquals("end time mismatch after round-trip", endMillis, readInner.getDataEndMillis());
    }

    /* ------------------------------------------------------------------
       Verify writeTo() delegates to inner request
       ------------------------------------------------------------------ */
    public void testWriteToDelegatesToInnerRequest() throws IOException {
        // spy around a minimal request so we can verify writeTo is called
        ADHCImputeRequest spyInner = spy(new ADHCImputeRequest("det", "task", 0L, 0L));

        ADHCImputeNodeRequest nodeReq = new ADHCImputeNodeRequest(spyInner);

        BytesStreamOutput out = new BytesStreamOutput();
        nodeReq.writeTo(out);

        // innerReq.writeTo(...) should have been invoked exactly once
        verify(spyInner).writeTo(any(BytesStreamOutput.class));
    }
}
