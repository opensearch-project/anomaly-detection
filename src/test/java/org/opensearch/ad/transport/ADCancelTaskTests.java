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

import static org.opensearch.ad.TestHelpers.randomDiscoveryNode;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.task.ADTaskCancellationState;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.StreamInput;

import com.google.common.collect.ImmutableList;

public class ADCancelTaskTests extends ADUnitTestCase {

    public void testADCancelTaskRequest() throws IOException {
        ADCancelTaskRequest request = new ADCancelTaskRequest(
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomDiscoveryNode()
        );

        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ADCancelTaskRequest parsedRequest = new ADCancelTaskRequest(input);
        assertEquals(request.getDetectorId(), parsedRequest.getDetectorId());
        assertEquals(request.getUserName(), parsedRequest.getUserName());
    }

    public void testInvalidADCancelTaskRequest() {
        ADCancelTaskRequest request = new ADCancelTaskRequest(null, null, null, randomDiscoveryNode());
        ActionRequestValidationException validationException = request.validate();
        assertTrue(validationException.getMessage().contains(ADCommonMessages.AD_ID_MISSING_MSG));
    }

    public void testSerializeResponse() throws IOException {
        ADTaskCancellationState state = ADTaskCancellationState.CANCELLED;
        ADCancelTaskNodeResponse nodeResponse = new ADCancelTaskNodeResponse(randomDiscoveryNode(), state);

        List<ADCancelTaskNodeResponse> nodes = ImmutableList.of(nodeResponse);
        ADCancelTaskResponse response = new ADCancelTaskResponse(new ClusterName("test"), nodes, ImmutableList.of());

        BytesStreamOutput output = new BytesStreamOutput();
        response.writeNodesTo(output, nodes);
        StreamInput input = output.bytes().streamInput();

        List<ADCancelTaskNodeResponse> adCancelTaskNodeResponses = response.readNodesFrom(input);
        assertEquals(1, adCancelTaskNodeResponses.size());
        assertEquals(state, adCancelTaskNodeResponses.get(0).getState());

        BytesStreamOutput output2 = new BytesStreamOutput();
        response.writeTo(output2);
        StreamInput input2 = output2.bytes().streamInput();

        ADCancelTaskResponse response2 = new ADCancelTaskResponse(input2);
        assertEquals(1, response2.getNodes().size());
        assertEquals(state, response2.getNodes().get(0).getState());
    }
}
