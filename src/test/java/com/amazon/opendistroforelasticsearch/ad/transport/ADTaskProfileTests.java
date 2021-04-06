/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.transport;

import static com.amazon.opendistroforelasticsearch.ad.TestHelpers.randomDiscoveryNode;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskProfile;
import com.google.common.collect.ImmutableList;

public class ADTaskProfileTests extends OpenSearchSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, AnomalyDetectorPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testADTaskProfileRequest() throws IOException {
        ADTaskProfileRequest request = new ADTaskProfileRequest(randomAlphaOfLength(5), randomDiscoveryNode());

        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ADTaskProfileRequest parsedRequest = new ADTaskProfileRequest(input);
        assertEquals(request.getDetectorId(), parsedRequest.getDetectorId());
    }

    public void testInvalidADTaskProfileRequest() {
        DiscoveryNode node = new DiscoveryNode(UUIDs.randomBase64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        ADTaskProfileRequest request = new ADTaskProfileRequest(null, node);
        ActionRequestValidationException validationException = request.validate();
        assertTrue(validationException.getMessage().contains(CommonErrorMessages.AD_ID_MISSING_MSG));
    }

    public void testADTaskProfileNodeResponse() throws IOException {
        ADTaskProfile adTaskProfile = new ADTaskProfile(
            randomInt(),
            randomLong(),
            randomBoolean(),
            randomInt(),
            randomLong(),
            randomAlphaOfLength(5)
        );
        ADTaskProfileNodeResponse response = new ADTaskProfileNodeResponse(randomDiscoveryNode(), adTaskProfile);
        testADTaskProfileResponse(response);
    }

    public void testADTaskProfileNodeResponseWithNullProfile() throws IOException {
        ADTaskProfileNodeResponse response = new ADTaskProfileNodeResponse(randomDiscoveryNode(), null);
        testADTaskProfileResponse(response);
    }

    public void testADTaskProfileNodeResponseReadMethod() throws IOException {
        ADTaskProfile adTaskProfile = new ADTaskProfile(
            randomInt(),
            randomLong(),
            randomBoolean(),
            randomInt(),
            randomLong(),
            randomAlphaOfLength(5)
        );
        ADTaskProfileNodeResponse response = new ADTaskProfileNodeResponse(randomDiscoveryNode(), adTaskProfile);
        testADTaskProfileResponse(response);
    }

    public void testADTaskProfileNodeResponseReadMethodWithNullProfile() throws IOException {
        ADTaskProfileNodeResponse response = new ADTaskProfileNodeResponse(randomDiscoveryNode(), null);
        testADTaskProfileResponse(response);
    }

    private void testADTaskProfileResponse(ADTaskProfileNodeResponse response) throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ADTaskProfileNodeResponse parsedResponse = ADTaskProfileNodeResponse.readNodeResponse(input);
        if (response.getAdTaskProfile() != null) {
            assertTrue(response.getAdTaskProfile().equals(parsedResponse.getAdTaskProfile()));
        } else {
            assertNull(parsedResponse.getAdTaskProfile());
        }
    }

    public void testSerializeResponse() throws IOException {
        DiscoveryNode node = randomDiscoveryNode();
        ADTaskProfile profile = new ADTaskProfile(
            TestHelpers.randomAdTask(),
            randomInt(),
            randomLong(),
            randomBoolean(),
            randomInt(),
            randomLong(),
            randomAlphaOfLength(5)
        );
        ADTaskProfileNodeResponse nodeResponse = new ADTaskProfileNodeResponse(node, profile);
        ImmutableList<ADTaskProfileNodeResponse> nodes = ImmutableList.of(nodeResponse);
        ADTaskProfileResponse response = new ADTaskProfileResponse(new ClusterName("test"), nodes, ImmutableList.of());

        BytesStreamOutput output = new BytesStreamOutput();
        response.writeNodesTo(output, nodes);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());

        List<ADTaskProfileNodeResponse> adTaskProfileNodeResponses = response.readNodesFrom(input);
        assertEquals(1, adTaskProfileNodeResponses.size());
        assertEquals(profile, adTaskProfileNodeResponses.get(0).getAdTaskProfile());

        BytesStreamOutput output2 = new BytesStreamOutput();
        response.writeTo(output2);
        NamedWriteableAwareStreamInput input2 = new NamedWriteableAwareStreamInput(output2.bytes().streamInput(), writableRegistry());

        ADTaskProfileResponse response2 = new ADTaskProfileResponse(input2);
        assertEquals(1, response2.getNodes().size());
        assertEquals(profile, response2.getNodes().get(0).getAdTaskProfile());
    }
}
