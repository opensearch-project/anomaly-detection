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

package org.opensearch;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensearch.action.FailedNodeException;
import org.opensearch.ad.model.EntityProfileName;
import org.opensearch.ad.model.ModelProfile;
import org.opensearch.ad.model.ModelProfileOnNode;
import org.opensearch.ad.transport.EntityProfileAction;
import org.opensearch.ad.transport.EntityProfileRequest;
import org.opensearch.ad.transport.EntityProfileResponse;
import org.opensearch.ad.transport.EntityResultRequest;
import org.opensearch.ad.transport.ProfileNodeResponse;
import org.opensearch.ad.transport.ProfileResponse;
import org.opensearch.ad.transport.RCFResultResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.model.Entity;

/**
 * Put in core package so that we can using Version's package private constructor
 *
 */
public class StreamInputOutputTests extends AbstractTimeSeriesTest {
    // public static Version V_1_1_0 = new Version(1010099, org.apache.lucene.util.Version.LUCENE_8_8_2);
    private EntityResultRequest entityResultRequest;
    private String detectorId;
    private long start, end;
    private Map<Entity, double[]> entities;
    private BytesStreamOutput output;
    private String categoryField, categoryValue, categoryValue2;
    private double[] feature;
    private EntityProfileRequest entityProfileRequest;
    private Entity entity, entity2;
    private Set<EntityProfileName> profilesToCollect;
    private String nodeId = "abc";
    private String modelId = "123";
    private long modelSize = 712480L;
    private long modelSize2 = 112480L;
    private EntityProfileResponse entityProfileResponse;
    private ProfileResponse profileResponse;
    private RCFResultResponse rcfResultResponse;

    private boolean areEqualWithArrayValue(Map<Entity, double[]> first, Map<Entity, double[]> second) {
        if (first.size() != second.size()) {
            return false;
        }

        return first.entrySet().stream().allMatch(e -> Arrays.equals(e.getValue(), second.get(e.getKey())));
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        categoryField = "a";
        categoryValue = "b";
        categoryValue2 = "b2";

        feature = new double[] { 0.3 };
        detectorId = "123";

        entity = Entity.createSingleAttributeEntity(categoryField, categoryValue);
        entity2 = Entity.createSingleAttributeEntity(categoryField, categoryValue2);

        output = new BytesStreamOutput();
    }

    private void setUpEntityResultRequest() {
        entities = new HashMap<>();
        entities.put(entity, feature);
        start = 10L;
        end = 20L;
        entityResultRequest = new EntityResultRequest(detectorId, entities, start, end);
    }

    /**
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testDeSerializeEntityResultRequest() throws IOException {
        setUpEntityResultRequest();

        entityResultRequest.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        EntityResultRequest readRequest = new EntityResultRequest(streamInput);
        assertThat(readRequest.getId(), equalTo(detectorId));
        assertThat(readRequest.getStart(), equalTo(start));
        assertThat(readRequest.getEnd(), equalTo(end));
        assertTrue(areEqualWithArrayValue(readRequest.getEntities(), entities));
    }

    private void setUpEntityProfileRequest() {
        profilesToCollect = new HashSet<EntityProfileName>();
        profilesToCollect.add(EntityProfileName.STATE);
        entityProfileRequest = new EntityProfileRequest(detectorId, entity, profilesToCollect);
    }

    /**
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testDeserializeEntityProfileRequest() throws IOException {
        setUpEntityProfileRequest();

        entityProfileRequest.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        EntityProfileRequest readRequest = new EntityProfileRequest(streamInput);
        assertThat(readRequest.getAdID(), equalTo(detectorId));
        assertThat(readRequest.getEntityValue(), equalTo(entity));
        assertThat(readRequest.getProfilesToCollect(), equalTo(profilesToCollect));
    }

    private void setUpEntityProfileResponse() {
        long lastActiveTimestamp = 10L;
        EntityProfileResponse.Builder builder = new EntityProfileResponse.Builder();
        builder.setLastActiveMs(lastActiveTimestamp).build();
        ModelProfile modelProfile = new ModelProfile(modelId, entity, modelSize);
        ModelProfileOnNode model = new ModelProfileOnNode(nodeId, modelProfile);
        builder.setModelProfile(model);
        entityProfileResponse = builder.build();
    }

    /**
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testDeserializeEntityProfileResponse() throws IOException {
        setUpEntityProfileResponse();

        entityProfileResponse.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        EntityProfileResponse readResponse = EntityProfileAction.INSTANCE.getResponseReader().read(streamInput);
        assertThat(readResponse.getModelProfile(), equalTo(entityProfileResponse.getModelProfile()));
        assertThat(readResponse.getLastActiveMs(), equalTo(entityProfileResponse.getLastActiveMs()));
        assertThat(readResponse.getTotalUpdates(), equalTo(entityProfileResponse.getTotalUpdates()));
    }

    @SuppressWarnings("serial")
    private void setUpProfileResponse() {
        String node1 = "node1";
        String nodeName1 = "nodename1";
        DiscoveryNode discoveryNode1_1 = new DiscoveryNode(
            nodeName1,
            node1,
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            emptyMap(),
            emptySet(),
            Version.V_2_1_0
        );

        String node2 = "node2";
        String nodeName2 = "nodename2";
        DiscoveryNode discoveryNode2 = new DiscoveryNode(
            nodeName2,
            node2,
            new TransportAddress(TransportAddress.META_ADDRESS, 9301),
            emptyMap(),
            emptySet(),
            Version.V_2_1_0
        );

        String model1Id = "model1";
        String model2Id = "model2";

        Map<String, Long> modelSizeMap1 = new HashMap<String, Long>() {
            {
                put(model1Id, modelSize);
                put(model2Id, modelSize2);
            }
        };
        Map<String, Long> modelSizeMap2 = new HashMap<String, Long>();

        int shingleSize = 8;

        ModelProfile modelProfile = new ModelProfile(model1Id, entity, modelSize);
        ModelProfile modelProfile2 = new ModelProfile(model2Id, entity2, modelSize2);

        ProfileNodeResponse profileNodeResponse1 = new ProfileNodeResponse(
            discoveryNode1_1,
            modelSizeMap1,
            shingleSize,
            0,
            0,
            Arrays.asList(modelProfile, modelProfile2),
            modelSizeMap1.size()
        );
        ProfileNodeResponse profileNodeResponse2 = new ProfileNodeResponse(
            discoveryNode2,
            modelSizeMap2,
            -1,
            0,
            0,
            new ArrayList<>(),
            modelSizeMap2.size()
        );
        ProfileNodeResponse profileNodeResponse3 = new ProfileNodeResponse(
            discoveryNode2,
            null,
            -1,
            0,
            0,
            // null model size. Test if we can handle this case
            null,
            modelSizeMap2.size()
        );
        List<ProfileNodeResponse> profileNodeResponses = Arrays.asList(profileNodeResponse1, profileNodeResponse2, profileNodeResponse3);
        List<FailedNodeException> failures = Collections.emptyList();

        ClusterName clusterName = new ClusterName("test-cluster-name");
        profileResponse = new ProfileResponse(clusterName, profileNodeResponses, failures);
    }

    /**
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testDeserializeProfileResponse() throws IOException {
        setUpProfileResponse();

        profileResponse.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        ProfileResponse readResponse = new ProfileResponse(streamInput);
        assertThat(readResponse.getModelProfile(), equalTo(profileResponse.getModelProfile()));
        assertThat(readResponse.getShingleSize(), equalTo(profileResponse.getShingleSize()));
        assertThat(readResponse.getActiveEntities(), equalTo(profileResponse.getActiveEntities()));
        assertThat(readResponse.getTotalUpdates(), equalTo(profileResponse.getTotalUpdates()));
        assertThat(readResponse.getCoordinatingNode(), equalTo(profileResponse.getCoordinatingNode()));
        assertThat(readResponse.getTotalSizeInBytes(), equalTo(profileResponse.getTotalSizeInBytes()));
        assertThat(readResponse.getModelCount(), equalTo(profileResponse.getModelCount()));
    }

    private void setUpRCFResultResponse() {
        rcfResultResponse = new RCFResultResponse(
            0.345,
            0.123,
            30,
            new double[] { 0.3, 0.7 },
            134,
            0.4,
            Version.CURRENT,
            randomIntBetween(-3, 0),
            new double[] { randomDoubleBetween(0, 1.0, true), randomDoubleBetween(0, 1.0, true) },
            new double[][] { new double[] { randomDouble(), randomDouble() } },
            new double[] { randomDoubleBetween(0, 1.0, true), randomDoubleBetween(0, 1.0, true) },
            randomDoubleBetween(1.1, 10.0, true)
        );
    }

    /**
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testDeserializeRCFResultResponse() throws IOException {
        setUpRCFResultResponse();

        rcfResultResponse.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        RCFResultResponse readResponse = new RCFResultResponse(streamInput);
        assertArrayEquals(readResponse.getAttribution(), rcfResultResponse.getAttribution(), 0.001);
        assertThat(readResponse.getConfidence(), equalTo(rcfResultResponse.getConfidence()));
        assertThat(readResponse.getForestSize(), equalTo(rcfResultResponse.getForestSize()));
        assertThat(readResponse.getTotalUpdates(), equalTo(rcfResultResponse.getTotalUpdates()));
        assertThat(readResponse.getRCFScore(), equalTo(rcfResultResponse.getRCFScore()));
    }
}
