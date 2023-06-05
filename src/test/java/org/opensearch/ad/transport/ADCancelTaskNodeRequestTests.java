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

import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.ad.mock.transport.MockADCancelTaskNodeRequest_1_0;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;

public class ADCancelTaskNodeRequestTests extends ADUnitTestCase {

    public void testParseOldADCancelTaskNodeRequestTest() throws IOException {
        String detectorId = randomAlphaOfLength(5);
        String userName = randomAlphaOfLength(5);
        MockADCancelTaskNodeRequest_1_0 oldRequest = new MockADCancelTaskNodeRequest_1_0(detectorId, userName);
        BytesStreamOutput output = new BytesStreamOutput();
        oldRequest.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        ADCancelTaskNodeRequest parsedRequest = new ADCancelTaskNodeRequest(input);
        assertEquals(detectorId, parsedRequest.getId());
        assertEquals(userName, parsedRequest.getUserName());
        assertNull(parsedRequest.getDetectorTaskId());
        assertNull(parsedRequest.getReason());
    }
}
