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

import static org.opensearch.timeseries.TestHelpers.randomDiscoveryNode;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;

import org.junit.Ignore;
import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.timeseries.TaskProfile;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;

import com.google.common.collect.ImmutableList;

public class ADTaskProfileTests extends OpenSearchSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, TimeSeriesAnalyticsPlugin.class);
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
        assertEquals(request.getId(), parsedRequest.getId());
    }

    public void testInvalidADTaskProfileRequest() {
        DiscoveryNode node = new DiscoveryNode(UUIDs.randomBase64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        ADTaskProfileRequest request = new ADTaskProfileRequest(null, node);
        ActionRequestValidationException validationException = request.validate();
        assertTrue(validationException.getMessage().contains(ADCommonMessages.AD_ID_MISSING_MSG));
    }

    public void testADTaskProfileNodeResponse() throws IOException {
        ADTaskProfile adTaskProfile = new ADTaskProfile(
            randomAlphaOfLength(5),
            randomLong(),
            randomBoolean(),
            randomInt(),
            randomLong(),
            randomAlphaOfLength(5)
        );
        ADTaskProfileNodeResponse response = new ADTaskProfileNodeResponse(randomDiscoveryNode(), adTaskProfile, Version.CURRENT);
        testADTaskProfileResponse(response);
    }

    public void testADTaskProfileNodeResponseWithNullProfile() throws IOException {
        ADTaskProfileNodeResponse response = new ADTaskProfileNodeResponse(randomDiscoveryNode(), null, Version.CURRENT);
        testADTaskProfileResponse(response);
    }

    public void testADTaskProfileNodeResponseReadMethod() throws IOException {
        ADTaskProfile adTaskProfile = new ADTaskProfile(
            randomAlphaOfLength(5),
            randomLong(),
            randomBoolean(),
            randomInt(),
            randomLong(),
            randomAlphaOfLength(5)
        );
        ADTaskProfileNodeResponse response = new ADTaskProfileNodeResponse(randomDiscoveryNode(), adTaskProfile, Version.CURRENT);
        testADTaskProfileResponse(response);
    }

    public void testADTaskProfileNodeResponseReadMethodWithNullProfile() throws IOException {
        ADTaskProfileNodeResponse response = new ADTaskProfileNodeResponse(randomDiscoveryNode(), null, Version.CURRENT);
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

    public void testADTaskProfileParse() throws IOException {
        TaskProfile adTaskProfile = new ADTaskProfile(
            randomAlphaOfLength(5),
            randomLong(),
            randomBoolean(),
            randomInt(),
            randomLong(),
            randomAlphaOfLength(5)
        );
        String adTaskProfileString = TestHelpers
            .xContentBuilderToString(adTaskProfile.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        TaskProfile parsedADTaskProfile = ADTaskProfile.parse(TestHelpers.parser(adTaskProfileString));
        assertEquals(adTaskProfile, parsedADTaskProfile);
        assertEquals(parsedADTaskProfile.toString(), adTaskProfile.toString());
    }

    @Ignore
    public void testSerializeResponse() throws IOException {
        DiscoveryNode node = randomDiscoveryNode();
        ADTaskProfile profile = new ADTaskProfile(
            TestHelpers.randomAdTask(),
            randomLong(),
            randomBoolean(),
            randomInt(),
            randomLong(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomInt(),
            randomBoolean(),
            randomInt(),
            randomInt(),
            randomInt(),
            ImmutableList.of(randomAlphaOfLength(5)),
            Instant.now().toEpochMilli()
        );
        ADTaskProfileNodeResponse nodeResponse = new ADTaskProfileNodeResponse(node, profile, Version.CURRENT);
        ImmutableList<ADTaskProfileNodeResponse> nodes = ImmutableList.of(nodeResponse);
        ADTaskProfileResponse response = new ADTaskProfileResponse(new ClusterName("test"), nodes, ImmutableList.of());

        BytesStreamOutput output = new BytesStreamOutput();
        response.writeNodesTo(output, nodes);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());

        List<ADTaskProfileNodeResponse> adTaskProfileNodeResponses = response.readNodesFrom(input);
        assertEquals(1, adTaskProfileNodeResponses.size());
        ADTaskProfileNodeResponse parsedProfile = adTaskProfileNodeResponses.get(0);

        assertEquals(profile.getTaskId(), parsedProfile.getAdTaskProfile().getTaskId());
    }

    public void testADTaskProfileParseFullConstructor() throws IOException {
        TaskProfile adTaskProfile = new ADTaskProfile(
            TestHelpers.randomAdTask(),
            randomLong(),
            randomBoolean(),
            randomInt(),
            randomLong(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomInt(),
            randomBoolean(),
            randomInt(),
            randomInt(),
            randomInt(),
            ImmutableList.of(randomAlphaOfLength(5)),
            Instant.now().toEpochMilli()
        );
        String adTaskProfileString = TestHelpers
            .xContentBuilderToString(adTaskProfile.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        TaskProfile parsedADTaskProfile = ADTaskProfile.parse(TestHelpers.parser(adTaskProfileString));
        assertEquals(adTaskProfile, parsedADTaskProfile);
    }
}
