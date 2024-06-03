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

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.ad.common.exception.JsonPathNotFoundException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.ModelProfileOnNode;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.transport.ProfileNodeRequest;
import org.opensearch.timeseries.transport.ProfileNodeResponse;
import org.opensearch.timeseries.transport.ProfileRequest;
import org.opensearch.timeseries.transport.ProfileResponse;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import test.org.opensearch.ad.util.JsonDeserializer;

public class ProfileTests extends OpenSearchTestCase {
    String node1, nodeName1, clusterName;
    String node2, nodeName2;
    Map<String, Object> clusterStats;
    DiscoveryNode discoveryNode1, discoveryNode2;
    long modelSize;
    String model1Id;
    String model0Id;
    String detectorId;
    int shingleSize;
    Map<String, Long> modelSizeMap1, modelSizeMap2;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterName = "test-cluster-name";

        node1 = "node1";
        nodeName1 = "nodename1";
        discoveryNode1 = new DiscoveryNode(
            nodeName1,
            node1,
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );

        node2 = "node2";
        nodeName2 = "nodename2";
        discoveryNode2 = new DiscoveryNode(
            nodeName2,
            node2,
            new TransportAddress(TransportAddress.META_ADDRESS, 9301),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );

        clusterStats = new HashMap<>();

        modelSize = 4456448L;
        model1Id = "Pl536HEBnXkDrah03glg_model_rcf_1";
        model0Id = "Pl536HEBnXkDrah03glg_model_rcf_0";
        detectorId = "123";
        shingleSize = 6;

        modelSizeMap1 = new HashMap<String, Long>() {
            {
                put(model1Id, modelSize);
            }
        };

        modelSizeMap2 = new HashMap<String, Long>() {
            {
                put(model0Id, modelSize);
            }
        };
    }

    @Test
    public void testProfileNodeRequest() throws IOException {

        Set<ProfileName> profilesToRetrieve = new HashSet<ProfileName>();
        profilesToRetrieve.add(ProfileName.COORDINATING_NODE);
        ProfileRequest ProfileRequest = new ProfileRequest(detectorId, profilesToRetrieve);
        ProfileNodeRequest ProfileNodeRequest = new ProfileNodeRequest(ProfileRequest);
        assertEquals("ProfileNodeRequest has the wrong detector id", ProfileNodeRequest.getConfigId(), detectorId);
        assertEquals("ProfileNodeRequest has the wrong ProfileRequest", ProfileNodeRequest.getProfilesToBeRetrieved(), profilesToRetrieve);

        // Test serialization
        BytesStreamOutput output = new BytesStreamOutput();
        ProfileNodeRequest.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        ProfileNodeRequest nodeRequest = new ProfileNodeRequest(streamInput);
        assertEquals("serialization has the wrong detector id", nodeRequest.getConfigId(), detectorId);
        assertEquals("serialization has the wrong ProfileRequest", nodeRequest.getProfilesToBeRetrieved(), profilesToRetrieve);

    }

    @Test
    public void testProfileNodeResponse() throws IOException, JsonPathNotFoundException {

        // Test serialization
        ProfileNodeResponse profileNodeResponse = new ProfileNodeResponse(
            discoveryNode1,
            modelSizeMap1,
            0,
            0,
            new ArrayList<>(),
            modelSizeMap1.size(),
            false
        );
        BytesStreamOutput output = new BytesStreamOutput();
        profileNodeResponse.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        ProfileNodeResponse readResponse = ProfileNodeResponse.readProfiles(streamInput);
        assertEquals("serialization has the wrong model size", readResponse.getModelSize(), profileNodeResponse.getModelSize());

        // Test toXContent
        XContentBuilder builder = jsonBuilder();
        profileNodeResponse.toXContent(builder.startObject(), ToXContent.EMPTY_PARAMS).endObject();
        String json = builder.toString();

        for (Map.Entry<String, Long> profile : modelSizeMap1.entrySet()) {
            assertEquals(
                "toXContent has the wrong model size",
                JsonDeserializer.getLongValue(json, CommonName.MODEL_SIZE_IN_BYTES, profile.getKey()),
                profile.getValue().longValue()
            );
        }

    }

    @Test
    public void testProfileRequest() throws IOException {
        String detectorId = "123";
        Set<ProfileName> profilesToRetrieve = new HashSet<ProfileName>();
        profilesToRetrieve.add(ProfileName.COORDINATING_NODE);
        ProfileRequest profileRequest = new ProfileRequest(detectorId, profilesToRetrieve);

        // Test Serialization
        BytesStreamOutput output = new BytesStreamOutput();
        profileRequest.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        ProfileRequest readRequest = new ProfileRequest(streamInput);
        assertEquals(
            "Serialization has the wrong profiles to be retrieved",
            readRequest.getProfilesToBeRetrieved(),
            profileRequest.getProfilesToBeRetrieved()
        );
        assertEquals("Serialization has the wrong detector id", readRequest.getConfigId(), profileRequest.getConfigId());
    }

    @Test
    public void testProfileResponse() throws IOException, JsonPathNotFoundException {

        ProfileNodeResponse profileNodeResponse1 = new ProfileNodeResponse(
            discoveryNode1,
            modelSizeMap1,
            0,
            0,
            new ArrayList<>(),
            modelSizeMap1.size(),
            true
        );
        ProfileNodeResponse profileNodeResponse2 = new ProfileNodeResponse(
            discoveryNode2,
            modelSizeMap2,
            0,
            0,
            new ArrayList<>(),
            modelSizeMap2.size(),
            false
        );
        List<ProfileNodeResponse> profileNodeResponses = Arrays.asList(profileNodeResponse1, profileNodeResponse2);
        List<FailedNodeException> failures = Collections.emptyList();
        ProfileResponse profileResponse = new ProfileResponse(new ClusterName(clusterName), profileNodeResponses, failures);

        assertEquals(node1, profileResponse.getCoordinatingNode());
        assertEquals(modelSize * 2, profileResponse.getTotalSizeInBytes());
        assertEquals(2, profileResponse.getModelProfile().length);
        for (ModelProfileOnNode profile : profileResponse.getModelProfile()) {
            assertTrue(node1.equals(profile.getNodeId()) || node2.equals(profile.getNodeId()));
            assertEquals(modelSize, profile.getModelSize());
            if (node1.equals(profile.getNodeId())) {
                assertEquals(model1Id, profile.getModelId());
            }
            if (node2.equals(profile.getNodeId())) {
                assertEquals(model0Id, profile.getModelId());
            }
        }

        // Test toXContent
        XContentBuilder builder = jsonBuilder();
        profileResponse.toXContent(builder.startObject(), ToXContent.EMPTY_PARAMS).endObject();
        String json = builder.toString();

        logger.info("JSON: " + json);

        assertEquals(
            "toXContent has the wrong coordinating node",
            node1,
            JsonDeserializer.getTextValue(json, ProfileResponse.COORDINATING_NODE)
        );

        assertEquals("toXContent has the wrong total size", modelSize * 2, JsonDeserializer.getLongValue(json, ProfileResponse.TOTAL_SIZE));

        JsonArray modelsJson = JsonDeserializer.getArrayValue(json, ProfileResponse.MODELS);

        for (int i = 0; i < modelsJson.size(); i++) {
            JsonElement element = modelsJson.get(i);
            assertTrue(
                "toXContent has the wrong model id",
                JsonDeserializer.getTextValue(element, CommonName.MODEL_ID_FIELD).equals(model1Id)
                    || JsonDeserializer.getTextValue(element, CommonName.MODEL_ID_FIELD).equals(model0Id)
            );

            assertEquals(
                "toXContent has the wrong model size",
                JsonDeserializer.getLongValue(element, CommonName.MODEL_SIZE_IN_BYTES),
                modelSize
            );

            if (JsonDeserializer.getTextValue(element, CommonName.MODEL_ID_FIELD).equals(model1Id)) {
                assertEquals("toXContent has the wrong node id", JsonDeserializer.getTextValue(element, ModelProfileOnNode.NODE_ID), node1);
            } else {
                assertEquals("toXContent has the wrong node id", JsonDeserializer.getTextValue(element, ModelProfileOnNode.NODE_ID), node2);
            }

        }
    }
}
