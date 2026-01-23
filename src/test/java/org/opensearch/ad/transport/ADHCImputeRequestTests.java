/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.io.IOException;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class ADHCImputeRequestTests extends OpenSearchTestCase {

    public void testRoundTripSerialization() throws IOException {
        String configId = "detector-id";
        String taskId = "task-id";
        long startMillis = 10L;
        long endMillis = 20L;
        ADHCImputeRequest request = new ADHCImputeRequest(configId, taskId, startMillis, endMillis);

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        ADHCImputeRequest parsed = new ADHCImputeRequest(input);

        assertEquals(configId, parsed.getConfigId());
        assertEquals(taskId, parsed.getTaskId());
        assertEquals(startMillis, parsed.getDataStartMillis());
        assertEquals(endMillis, parsed.getDataEndMillis());
    }

    public void testRoundTripSerializationWithNullTaskId() throws IOException {
        String configId = "detector-id";
        long startMillis = 100L;
        long endMillis = 200L;
        ADHCImputeRequest request = new ADHCImputeRequest(configId, null, startMillis, endMillis);

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        ADHCImputeRequest parsed = new ADHCImputeRequest(input);

        assertEquals(configId, parsed.getConfigId());
        assertNull(parsed.getTaskId());
        assertEquals(startMillis, parsed.getDataStartMillis());
        assertEquals(endMillis, parsed.getDataEndMillis());
    }
}
