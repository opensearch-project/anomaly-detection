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

import org.opensearch.Version;
import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import com.google.common.collect.ImmutableList;

public class ADTaskProfileResponseTests extends ADUnitTestCase {

    public void testSerializeResponse() throws IOException {
        String taskId = randomAlphaOfLength(5);
        ADTaskProfile adTaskProfile = new ADTaskProfile();
        adTaskProfile.setTaskId(taskId);
        Version remoteAdVersion = Version.CURRENT;
        ADTaskProfileNodeResponse nodeResponse = new ADTaskProfileNodeResponse(randomDiscoveryNode(), adTaskProfile, remoteAdVersion);

        List<ADTaskProfileNodeResponse> nodeResponses = ImmutableList.of(nodeResponse);
        ADTaskProfileResponse response = new ADTaskProfileResponse(new ClusterName("test"), nodeResponses, ImmutableList.of());

        BytesStreamOutput output = new BytesStreamOutput();
        response.writeNodesTo(output, nodeResponses);
        StreamInput input = output.bytes().streamInput();

        List<ADTaskProfileNodeResponse> adTaskProfileNodeResponses = response.readNodesFrom(input);
        assertEquals(1, adTaskProfileNodeResponses.size());
        assertEquals(taskId, adTaskProfileNodeResponses.get(0).getAdTaskProfile().getTaskId());

        BytesStreamOutput output2 = new BytesStreamOutput();
        response.writeTo(output2);
        StreamInput input2 = output2.bytes().streamInput();

        ADTaskProfileResponse response2 = new ADTaskProfileResponse(input2);
        assertEquals(1, response2.getNodes().size());
        assertEquals(taskId, response2.getNodes().get(0).getAdTaskProfile().getTaskId());
    }

    public void testFromActionResponse() throws IOException {
        String taskId = randomAlphaOfLength(5);
        ADTaskProfile adTaskProfile = new ADTaskProfile();
        adTaskProfile.setTaskId(taskId);
        Version remoteAdVersion = Version.CURRENT;
        ADTaskProfileNodeResponse nodeResponse = new ADTaskProfileNodeResponse(randomDiscoveryNode(), adTaskProfile, remoteAdVersion);

        List<ADTaskProfileNodeResponse> nodeResponses = ImmutableList.of(nodeResponse);
        ADTaskProfileResponse response = new ADTaskProfileResponse(new ClusterName("test"), nodeResponses, ImmutableList.of());

        ADTaskProfileResponse reserializedResponse = ADTaskProfileResponse.fromActionResponse((ActionResponse) response);
        assertEquals(1, reserializedResponse.getNodes().size());
        assertEquals(taskId, reserializedResponse.getNodes().get(0).getAdTaskProfile().getTaskId());

        BytesStreamOutput output = new BytesStreamOutput();
        response.writeNodesTo(output, nodeResponses);
        StreamInput input = output.bytes().streamInput();

        ActionResponse invalidActionResponse = new TestActionResponse(input);
        assertThrows(Exception.class, () -> ADTaskProfileResponse.fromActionResponse(invalidActionResponse));

    }

    // A test ActionResponse class with an inactive writeTo class. Used to ensure exceptions
    // are thrown when parsing implementations of such class.
    private class TestActionResponse extends ActionResponse {
        public TestActionResponse(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            return;
        }
    }

}
