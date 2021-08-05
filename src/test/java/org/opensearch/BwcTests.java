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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.opensearch.action.FailedNodeException;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.EntityProfileName;
import org.opensearch.ad.model.FeatureData;
import org.opensearch.ad.model.ModelProfile;
import org.opensearch.ad.model.ModelProfileOnNode;
import org.opensearch.ad.transport.AnomalyResultResponse;
import org.opensearch.ad.transport.CronNodeRequest;
import org.opensearch.ad.transport.CronRequest;
import org.opensearch.ad.transport.EntityProfileAction;
import org.opensearch.ad.transport.EntityProfileRequest;
import org.opensearch.ad.transport.EntityProfileResponse;
import org.opensearch.ad.transport.EntityResultRequest;
import org.opensearch.ad.transport.ProfileNodeResponse;
import org.opensearch.ad.transport.ProfileResponse;
import org.opensearch.ad.transport.RCFResultResponse;
import org.opensearch.ad.util.Bwc;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.transport.TransportAddress;

/**
 * Put in core package so that we can using Version's package private constructor
 *
 */
public class BwcTests extends AbstractADTest {
    public static Version V_1_1_0 = new Version(1010099, org.apache.lucene.util.Version.LUCENE_8_8_2);
    private EntityResultRequest entityResultRequest1_1;
    private EntityResultRequest1_0 entityResultRequest1_0;
    private String detectorId;
    private long start, end;
    private Map<Entity, double[]> entities1_1, convertedEntities1_0;
    private Map<String, double[]> entities1_0;
    private BytesStreamOutput output1_1, output1_0;
    private String categoryField, categoryValue, categoryValue2;
    private double[] feature;
    private EntityProfileRequest entityProfileRequest1_1;
    private EntityProfileRequest1_0 entityProfileRequest1_0;
    private Entity entity, entity2, convertedEntity;
    private Set<EntityProfileName> profilesToCollect;
    private String nodeId = "abc";
    private String modelId = "123";
    private long modelSize = 712480L;
    private long modelSize2 = 112480L;
    private EntityProfileResponse entityProfileResponse1_1;
    private EntityProfileResponse1_0 entityProfileResponse1_0;
    private ModelProfileOnNode convertedModelProfileOnNode;
    private ProfileResponse profileResponse1_1;
    private ProfileResponse1_0 profileResponse1_0;
    private ModelProfileOnNode[] convertedModelProfileOnNodeArray;
    private ModelProfile1_0[] convertedModelProfile;
    private RCFResultResponse rcfResultResponse1_1;
    private RCFResultResponse1_0 rcfResultResponse1_0;
    private CronRequest cronRequest1_1;
    private CronRequest1_0 cronRequest1_0;
    private DiscoveryNode node;
    private CronNodeRequest cronNodeRequest1_1;
    private CronNodeRequest1_0 cronNodeRequest1_0;
    private AnomalyResultResponse adResultResponse1_1;
    private AnomalyResultResponse1_0 adResultResponse1_0;

    private boolean areEqualWithArrayValue(Map<Entity, double[]> first, Map<Entity, double[]> second) {
        if (first.size() != second.size()) {
            return false;
        }

        return first.entrySet().stream().allMatch(e -> Arrays.equals(e.getValue(), second.get(e.getKey())));
    }

    private boolean areEqualEntityArrayValue1_0(Map<String, double[]> first, Map<String, double[]> second) {
        if (first.size() != second.size()) {
            return false;
        }

        return first.entrySet().stream().allMatch(e -> Arrays.equals(e.getValue(), second.get(e.getKey())));
    }

    @BeforeClass
    public static void setUpBeforeClass() {
        Bwc.DISABLE_BWC = false;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        categoryField = "a";
        categoryValue = "b";
        categoryValue2 = "b2";

        feature = new double[] { 0.3 };
        detectorId = "123";

        entity = Entity.createSingleAttributeEntity(detectorId, categoryField, categoryValue);
        entity2 = Entity.createSingleAttributeEntity(detectorId, categoryField, categoryValue2);
        convertedEntity = Entity.createSingleAttributeEntity(detectorId, CommonName.EMPTY_FIELD, categoryValue);

        output1_1 = new BytesStreamOutput();
        output1_1.setVersion(V_1_1_0);

        output1_0 = new BytesStreamOutput();
        output1_0.setVersion(Version.V_1_0_0);
    }

    private void setUpEntityResultRequest() {
        entities1_1 = new HashMap<>();
        entities1_1.put(entity, feature);
        start = 10L;
        end = 20L;
        entityResultRequest1_1 = new EntityResultRequest(detectorId, entities1_1, start, end);

        entities1_0 = new HashMap<>();
        entities1_0.put(categoryValue, feature);
        entityResultRequest1_0 = new EntityResultRequest1_0(detectorId, entities1_0, start, end);
        convertedEntities1_0 = new HashMap<Entity, double[]>();
        convertedEntities1_0.put(convertedEntity, feature);
    }

    /**
     * For EntityResultRequest, the input is a 1.1 stream.
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testDeSerializeEntityResultRequest1_1() throws IOException {
        setUpEntityResultRequest();

        entityResultRequest1_1.writeTo(output1_1);

        StreamInput streamInput = output1_1.bytes().streamInput();
        streamInput.setVersion(V_1_1_0);
        EntityResultRequest readRequest = new EntityResultRequest(streamInput);
        assertThat(readRequest.getDetectorId(), equalTo(detectorId));
        assertThat(readRequest.getStart(), equalTo(start));
        assertThat(readRequest.getEnd(), equalTo(end));
        assertTrue(areEqualWithArrayValue(readRequest.getEntities(), entities1_1));
    }

    /**
     * For EntityResultRequest, the input is a 1.0 stream.
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testDeSerializeEntityResultRequest1_0() throws IOException {
        setUpEntityResultRequest();

        entityResultRequest1_0.writeTo(output1_0);

        StreamInput streamInput = output1_0.bytes().streamInput();
        streamInput.setVersion(Version.V_1_0_0);
        EntityResultRequest readRequest = new EntityResultRequest(streamInput);
        assertThat(readRequest.getDetectorId(), equalTo(detectorId));
        assertThat(readRequest.getStart(), equalTo(start));
        assertThat(readRequest.getEnd(), equalTo(end));
        assertTrue(areEqualWithArrayValue(readRequest.getEntities(), convertedEntities1_0));
    }

    /**
     * For EntityResultRequest, the output is a 1.0 stream.
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testSerializateEntityResultRequest1_0() throws IOException {
        setUpEntityResultRequest();

        entityResultRequest1_1.writeTo(output1_0);

        StreamInput streamInput = output1_0.bytes().streamInput();
        streamInput.setVersion(Version.V_1_0_0);
        EntityResultRequest1_0 readRequest = new EntityResultRequest1_0(streamInput);
        assertThat(readRequest.getDetectorId(), equalTo(detectorId));
        assertThat(readRequest.getStart(), equalTo(start));
        assertThat(readRequest.getEnd(), equalTo(end));
        assertTrue(areEqualEntityArrayValue1_0(readRequest.getEntities(), entityResultRequest1_0.getEntities()));
    }

    private void setUpEntityProfileRequest() {
        profilesToCollect = new HashSet<EntityProfileName>();
        profilesToCollect.add(EntityProfileName.STATE);
        entityProfileRequest1_1 = new EntityProfileRequest(detectorId, entity, profilesToCollect);
        entityProfileRequest1_0 = new EntityProfileRequest1_0(detectorId, categoryValue, profilesToCollect);
    }

    /**
     * For EntityResultRequest, the input is a 1.1 stream.
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testDeserializeEntityProfileRequest1_1() throws IOException {
        setUpEntityProfileRequest();

        entityProfileRequest1_1.writeTo(output1_1);

        StreamInput streamInput = output1_1.bytes().streamInput();
        streamInput.setVersion(V_1_1_0);
        EntityProfileRequest readRequest = new EntityProfileRequest(streamInput);
        assertThat(readRequest.getAdID(), equalTo(detectorId));
        assertThat(readRequest.getEntityValue(), equalTo(entity));
        assertThat(readRequest.getProfilesToCollect(), equalTo(profilesToCollect));
    }

    /**
     * For EntityResultRequest, the input is a 1.0 stream.
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testDeserializeEntityProfileRequest1_0() throws IOException {
        setUpEntityProfileRequest();

        entityProfileRequest1_0.writeTo(output1_0);

        StreamInput streamInput = output1_0.bytes().streamInput();
        streamInput.setVersion(Version.V_1_0_0);
        EntityProfileRequest readRequest = new EntityProfileRequest(streamInput);
        assertThat(readRequest.getAdID(), equalTo(detectorId));
        assertThat(readRequest.getEntityValue(), equalTo(convertedEntity));
        assertThat(readRequest.getProfilesToCollect(), equalTo(profilesToCollect));
    }

    /**
     * For EntityResultRequest, the output is a 1.0 stream.
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testSerializeEntityProfileRequest1_0() throws IOException {
        setUpEntityProfileRequest();

        entityProfileRequest1_1.writeTo(output1_0);

        StreamInput streamInput = output1_0.bytes().streamInput();
        streamInput.setVersion(Version.V_1_0_0);
        EntityProfileRequest1_0 readRequest = new EntityProfileRequest1_0(streamInput);
        assertThat(readRequest.getAdID(), equalTo(detectorId));
        assertThat(readRequest.getEntityValue(), equalTo(entity.toString()));
        assertThat(readRequest.getProfilesToCollect(), equalTo(profilesToCollect));
    }

    private void setUpEntityProfileResponse() {
        long lastActiveTimestamp = 10L;
        EntityProfileResponse.Builder builder = new EntityProfileResponse.Builder();
        builder.setLastActiveMs(lastActiveTimestamp).build();
        ModelProfile modelProfile = new ModelProfile(modelId, entity, modelSize);
        ModelProfileOnNode model = new ModelProfileOnNode(nodeId, modelProfile);
        builder.setModelProfile(model);
        entityProfileResponse1_1 = builder.build();

        EntityProfileResponse1_0.Builder builder1_0 = new EntityProfileResponse1_0.Builder();
        builder1_0.setLastActiveMs(lastActiveTimestamp).build();
        ModelProfile1_0 modelProfile1_0 = new ModelProfile1_0(modelId, modelSize, nodeId);
        builder1_0.setModelProfile(modelProfile1_0);
        entityProfileResponse1_0 = builder1_0.build();
        ModelProfile convertedModelProfile = new ModelProfile(modelId, null, modelSize);
        convertedModelProfileOnNode = new ModelProfileOnNode(CommonName.EMPTY_FIELD, convertedModelProfile);
    }

    /**
     * For EntityProfileResponse, the input is a 1.1 stream.
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testDeserializeEntityProfileResponse1_1() throws IOException {
        setUpEntityProfileResponse();

        entityProfileResponse1_1.writeTo(output1_1);

        StreamInput streamInput = output1_1.bytes().streamInput();
        streamInput.setVersion(V_1_1_0);
        EntityProfileResponse readResponse = EntityProfileAction.INSTANCE.getResponseReader().read(streamInput);
        assertThat(readResponse.getModelProfile(), equalTo(entityProfileResponse1_1.getModelProfile()));
        assertThat(readResponse.getLastActiveMs(), equalTo(entityProfileResponse1_1.getLastActiveMs()));
        assertThat(readResponse.getTotalUpdates(), equalTo(entityProfileResponse1_0.getTotalUpdates()));
    }

    /**
     * For EntityProfileResponse, the input is a 1.0 stream.
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testDeserializeEntityProfileResponse1_0() throws IOException {
        setUpEntityProfileResponse();

        entityProfileResponse1_0.writeTo(output1_0);

        StreamInput streamInput = output1_0.bytes().streamInput();
        streamInput.setVersion(Version.V_1_0_0);
        EntityProfileResponse readResponse = EntityProfileAction.INSTANCE.getResponseReader().read(streamInput);
        assertThat(readResponse.getModelProfile(), equalTo(convertedModelProfileOnNode));
        assertThat(readResponse.getLastActiveMs(), equalTo(entityProfileResponse1_0.getLastActiveMs()));
        assertThat(readResponse.getTotalUpdates(), equalTo(entityProfileResponse1_0.getTotalUpdates()));
    }

    /**
     * For EntityProfileResponse, the output is a 1.0 stream.
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testSerializeEntityProfileResponse1_0() throws IOException {
        setUpEntityProfileResponse();

        entityProfileResponse1_1.writeTo(output1_0);

        StreamInput streamInput = output1_0.bytes().streamInput();
        streamInput.setVersion(Version.V_1_0_0);
        EntityProfileResponse1_0 readResponse = new EntityProfileResponse1_0(streamInput);
        assertThat(readResponse.getModelProfile(), equalTo(new ModelProfile1_0(modelId, modelSize, CommonName.EMPTY_FIELD)));
        assertThat(readResponse.getLastActiveMs(), equalTo(entityProfileResponse1_1.getLastActiveMs()));
        assertThat(readResponse.getTotalUpdates(), equalTo(entityProfileResponse1_0.getTotalUpdates()));
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
            V_1_1_0
        );

        String node2 = "node2";
        String nodeName2 = "nodename2";
        DiscoveryNode discoveryNode2 = new DiscoveryNode(
            nodeName2,
            node2,
            new TransportAddress(TransportAddress.META_ADDRESS, 9301),
            emptyMap(),
            emptySet(),
            V_1_1_0
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
            Arrays.asList(modelProfile, modelProfile2)
        );
        ProfileNodeResponse profileNodeResponse2 = new ProfileNodeResponse(discoveryNode2, modelSizeMap2, -1, 0, 0, new ArrayList<>());
        List<ProfileNodeResponse> profileNodeResponses = Arrays.asList(profileNodeResponse1, profileNodeResponse2);
        List<FailedNodeException> failures = Collections.emptyList();

        ClusterName clusterName = new ClusterName("test-cluster-name");
        profileResponse1_1 = new ProfileResponse(clusterName, profileNodeResponses, failures);

        ProfileNodeResponse1_0 profileNodeResponse1_1_0 = new ProfileNodeResponse1_0(discoveryNode1_1, modelSizeMap1, shingleSize, 0, 0);
        ProfileNodeResponse1_0 profileNodeResponse2_1_0 = new ProfileNodeResponse1_0(discoveryNode2, modelSizeMap2, -1, 0, 0);
        List<ProfileNodeResponse1_0> profileNodeResponses1_0 = Arrays.asList(profileNodeResponse1_1_0, profileNodeResponse2_1_0);
        profileResponse1_0 = new ProfileResponse1_0(clusterName, profileNodeResponses1_0, failures);

        convertedModelProfileOnNodeArray = new ModelProfileOnNode[2];
        ModelProfile convertedModelProfile1 = new ModelProfile(model1Id, null, modelSize);
        convertedModelProfileOnNodeArray[0] = new ModelProfileOnNode(CommonName.EMPTY_FIELD, convertedModelProfile1);

        ModelProfile convertedModelProfile2 = new ModelProfile(model2Id, null, modelSize2);
        convertedModelProfileOnNodeArray[1] = new ModelProfileOnNode(CommonName.EMPTY_FIELD, convertedModelProfile2);

        convertedModelProfile = new ModelProfile1_0[2];
        convertedModelProfile[0] = new ModelProfile1_0(model1Id, modelSize, CommonName.EMPTY_FIELD);
        convertedModelProfile[1] = new ModelProfile1_0(model2Id, modelSize2, CommonName.EMPTY_FIELD);
    }

    /**
     * For ProfileResponse, the input is a 1.1 stream.
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testDeserializeProfileResponse1_1() throws IOException {
        setUpProfileResponse();

        profileResponse1_1.writeTo(output1_1);

        StreamInput streamInput = output1_1.bytes().streamInput();
        streamInput.setVersion(V_1_1_0);
        ProfileResponse readResponse = new ProfileResponse(streamInput);
        assertThat(readResponse.getModelProfile(), equalTo(profileResponse1_1.getModelProfile()));
        assertThat(readResponse.getShingleSize(), equalTo(profileResponse1_1.getShingleSize()));
        assertThat(readResponse.getActiveEntities(), equalTo(profileResponse1_1.getActiveEntities()));
        assertThat(readResponse.getTotalUpdates(), equalTo(profileResponse1_1.getTotalUpdates()));
        assertThat(readResponse.getCoordinatingNode(), equalTo(profileResponse1_1.getCoordinatingNode()));
        assertThat(readResponse.getTotalSizeInBytes(), equalTo(profileResponse1_1.getTotalSizeInBytes()));
    }

    /**
     * For ProfileResponse, the input is a 1.0 stream.
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testDeserializeProfileResponse1_0() throws IOException {
        setUpProfileResponse();

        profileResponse1_0.writeTo(output1_0);

        StreamInput streamInput = output1_0.bytes().streamInput();
        streamInput.setVersion(Version.V_1_0_0);
        ProfileResponse readResponse = new ProfileResponse(streamInput);
        ModelProfileOnNode[] actualModelProfileOnNode = readResponse.getModelProfile();

        // since ProfileResponse1_0's constructor iterates modelSize and modelSize is
        // a HashMap. The iteration order is not deterministic. We have to sort the
        // results in an ordered fashion to compare with expected value.
        Arrays.sort(actualModelProfileOnNode, new Comparator<ModelProfileOnNode>() {
            @Override
            public int compare(ModelProfileOnNode o1, ModelProfileOnNode o2) {
                return o1.getModelId().compareTo(o2.getModelId());
            }

        });
        assertThat(actualModelProfileOnNode, equalTo(convertedModelProfileOnNodeArray));
        assertThat(readResponse.getShingleSize(), equalTo(profileResponse1_1.getShingleSize()));
        assertThat(readResponse.getActiveEntities(), equalTo(profileResponse1_1.getActiveEntities()));
        assertThat(readResponse.getTotalUpdates(), equalTo(profileResponse1_1.getTotalUpdates()));
        assertThat(readResponse.getCoordinatingNode(), equalTo(profileResponse1_1.getCoordinatingNode()));
        assertThat(readResponse.getTotalSizeInBytes(), equalTo(profileResponse1_1.getTotalSizeInBytes()));
    }

    /**
     * For ProfileResponse, the output is a 1.0 stream.
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testSerializeProfileResponse1_0() throws IOException {
        setUpProfileResponse();

        profileResponse1_1.writeTo(output1_0);

        StreamInput streamInput = output1_0.bytes().streamInput();
        streamInput.setVersion(Version.V_1_0_0);
        ProfileResponse1_0 readResponse = new ProfileResponse1_0(streamInput);
        assertThat(readResponse.getModelProfile(), equalTo(convertedModelProfile));
        assertThat(readResponse.getShingleSize(), equalTo(profileResponse1_1.getShingleSize()));
        assertThat(readResponse.getActiveEntities(), equalTo(profileResponse1_1.getActiveEntities()));
        assertThat(readResponse.getTotalUpdates(), equalTo(profileResponse1_1.getTotalUpdates()));
        assertThat(readResponse.getCoordinatingNode(), equalTo(profileResponse1_1.getCoordinatingNode()));
        assertThat(readResponse.getTotalSizeInBytes(), equalTo(profileResponse1_1.getTotalSizeInBytes()));
    }

    /**
     * jacoco complained line coverage is 0.5, but we only have one line method
     * that is covered.  It flags the class name not covered.
     * Use solution mentioned in https://tinyurl.com/2pttzsd3
     */
    @SuppressWarnings("static-access")
    public void testBwcInstance() {
        Bwc bwc = new Bwc();
        assertNotNull(bwc);
    }

    private void setUpRCFResultResponse() {
        rcfResultResponse1_1 = new RCFResultResponse(0.345, 0.123, 30, new double[] { 0.3, 0.7 }, 134);
        rcfResultResponse1_0 = new RCFResultResponse1_0(0.345, 0.123, 30, new double[] { 0.3, 0.7 });
    }

    /**
     * For RCFResultResponse, the input is a 1.1 stream.
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testDeserializeRCFResultResponse1_1() throws IOException {
        setUpRCFResultResponse();

        rcfResultResponse1_1.writeTo(output1_1);

        StreamInput streamInput = output1_1.bytes().streamInput();
        streamInput.setVersion(V_1_1_0);
        RCFResultResponse readResponse = new RCFResultResponse(streamInput);
        assertArrayEquals(readResponse.getAttribution(), rcfResultResponse1_1.getAttribution(), 0.001);
        assertThat(readResponse.getConfidence(), equalTo(rcfResultResponse1_1.getConfidence()));
        assertThat(readResponse.getForestSize(), equalTo(rcfResultResponse1_1.getForestSize()));
        assertThat(readResponse.getTotalUpdates(), equalTo(rcfResultResponse1_1.getTotalUpdates()));
        assertThat(readResponse.getRCFScore(), equalTo(rcfResultResponse1_1.getRCFScore()));
    }

    /**
     * For RCFResultResponse, the input is a 1.0 stream.
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testDeserializeRCFResultResponse1_0() throws IOException {
        setUpRCFResultResponse();

        rcfResultResponse1_0.writeTo(output1_0);

        StreamInput streamInput = output1_0.bytes().streamInput();
        streamInput.setVersion(Version.V_1_0_0);
        RCFResultResponse readResponse = new RCFResultResponse(streamInput);
        assertArrayEquals(readResponse.getAttribution(), rcfResultResponse1_0.getAttribution(), 0.001);
        assertThat(readResponse.getConfidence(), equalTo(rcfResultResponse1_0.getConfidence()));
        assertThat(readResponse.getForestSize(), equalTo(rcfResultResponse1_0.getForestSize()));
        assertThat(readResponse.getTotalUpdates(), equalTo(0L));
        assertThat(readResponse.getRCFScore(), equalTo(rcfResultResponse1_0.getRCFScore()));
    }

    /**
     * For RCFResultResponse, the output is a 1.0 stream.
     * @throws IOException when serialization/deserialization has issues.
     */
    public void testSerializeRCFResultResponse1_0() throws IOException {
        setUpRCFResultResponse();

        rcfResultResponse1_1.writeTo(output1_0);

        StreamInput streamInput = output1_0.bytes().streamInput();
        streamInput.setVersion(Version.V_1_0_0);
        RCFResultResponse1_0 readResponse = new RCFResultResponse1_0(streamInput);
        assertArrayEquals(readResponse.getAttribution(), rcfResultResponse1_0.getAttribution(), 0.001);
        assertThat(readResponse.getConfidence(), equalTo(rcfResultResponse1_0.getConfidence()));
        assertThat(readResponse.getForestSize(), equalTo(rcfResultResponse1_0.getForestSize()));
        assertThat(readResponse.getRCFScore(), equalTo(rcfResultResponse1_0.getRCFScore()));
    }

    private void setUpCronRequest() throws UnknownHostException {
        node = new DiscoveryNode(
            nodeId,
            new TransportAddress(new InetSocketAddress(InetAddress.getByName("1.2.3.4"), 9300)),
            Version.V_1_0_0.minimumCompatibilityVersion()
        );
        cronRequest1_1 = new CronRequest("foo", node);
        cronRequest1_0 = new CronRequest1_0(node);
    }

    public void testDeserializeCronRequest1_1() throws IOException {
        setUpCronRequest();

        cronRequest1_1.writeTo(output1_1);

        StreamInput streamInput = output1_1.bytes().streamInput();
        streamInput.setVersion(V_1_1_0);
        CronRequest readRequest = new CronRequest(streamInput);
        assertEquals(readRequest.getRequestId(), cronRequest1_1.getRequestId());
        assertEquals(1, readRequest.concreteNodes().length);
        assertEquals(node, readRequest.concreteNodes()[0]);
    }

    public void testDeserializeCronRequest1_0() throws IOException {
        setUpCronRequest();

        cronRequest1_0.writeTo(output1_0);

        StreamInput streamInput = output1_0.bytes().streamInput();
        streamInput.setVersion(Version.V_1_0_0);
        CronRequest readRequest = new CronRequest(streamInput);
        assertEquals(1, readRequest.concreteNodes().length);
        assertEquals(node, readRequest.concreteNodes()[0]);
    }

    public void testSerializeCronRequest1_0() throws IOException {
        setUpCronRequest();

        cronRequest1_1.writeTo(output1_0);

        StreamInput streamInput = output1_0.bytes().streamInput();
        streamInput.setVersion(Version.V_1_0_0);
        CronRequest1_0 readRequest = new CronRequest1_0(streamInput);
        assertEquals(1, readRequest.concreteNodes().length);
        assertEquals(node, readRequest.concreteNodes()[0]);
    }

    private void setUpCronNodeRequest() {
        cronNodeRequest1_1 = new CronNodeRequest("blah");
        cronNodeRequest1_0 = new CronNodeRequest1_0();
    }

    public void testDeserializeCronNodeRequest1_1() throws IOException {
        setUpCronNodeRequest();

        cronNodeRequest1_1.writeTo(output1_1);

        StreamInput streamInput = output1_1.bytes().streamInput();
        streamInput.setVersion(V_1_1_0);
        CronNodeRequest readRequest = new CronNodeRequest(streamInput);
        assertEquals(readRequest.getRequestId(), cronNodeRequest1_1.getRequestId());
    }

    public void testDeserializeCronNodeRequest1_0() throws IOException {
        setUpCronNodeRequest();

        cronNodeRequest1_0.writeTo(output1_0);

        StreamInput streamInput = output1_0.bytes().streamInput();
        streamInput.setVersion(Version.V_1_0_0);
        CronNodeRequest readRequest = new CronNodeRequest(streamInput);
        assertTrue(readRequest != null);
    }

    public void testSerializeCronNodeRequest1_0() throws IOException {
        setUpCronNodeRequest();

        cronNodeRequest1_1.writeTo(output1_0);

        StreamInput streamInput = output1_0.bytes().streamInput();
        streamInput.setVersion(Version.V_1_0_0);
        CronNodeRequest1_0 readRequest = new CronNodeRequest1_0(streamInput);
        assertTrue(readRequest != null);
    }

    private void setUpAnomalyResultResponse() {
        adResultResponse1_1 = new AnomalyResultResponse(
            0.5,
            0.993,
            1.01,
            Collections.singletonList(new FeatureData("id", "name", 0d)),
            null,
            15L,
            10L
        );
        adResultResponse1_0 = new AnomalyResultResponse1_0(0.5, 0.993, 1.01, Collections.singletonList(new FeatureData("id", "name", 0d)));
    }

    public void testDeserializeAnomalyResultResponse1_1() throws IOException {
        setUpAnomalyResultResponse();

        adResultResponse1_1.writeTo(output1_1);

        StreamInput streamInput = output1_1.bytes().streamInput();
        streamInput.setVersion(V_1_1_0);
        AnomalyResultResponse readResponse = new AnomalyResultResponse(streamInput);
        assertEquals(readResponse.getAnomalyGrade(), adResultResponse1_1.getAnomalyGrade(), 0.001);
        assertEquals(readResponse.getConfidence(), adResultResponse1_1.getConfidence(), 0.001);
        assertEquals(readResponse.getAnomalyScore(), adResultResponse1_1.getAnomalyScore(), 0.001);
        assertEquals(1, readResponse.getFeatures().size());
        assertEquals(readResponse.getFeatures().get(0), adResultResponse1_1.getFeatures().get(0));
        assertEquals(readResponse.getError(), adResultResponse1_1.getError());
        assertEquals(readResponse.getRcfTotalUpdates(), adResultResponse1_1.getRcfTotalUpdates(), 0.001);
        assertEquals(readResponse.getDetectorIntervalInMinutes(), adResultResponse1_1.getDetectorIntervalInMinutes());
        assertEquals(readResponse.isHCDetector(), adResultResponse1_1.isHCDetector());
    }

    public void testDeserializeAnomalyResultResponse1_0() throws IOException {
        setUpAnomalyResultResponse();

        adResultResponse1_0.writeTo(output1_0);

        StreamInput streamInput = output1_0.bytes().streamInput();
        streamInput.setVersion(Version.V_1_0_0);
        AnomalyResultResponse readResponse = new AnomalyResultResponse(streamInput);
        assertEquals(readResponse.getAnomalyGrade(), adResultResponse1_1.getAnomalyGrade(), 0.001);
        assertEquals(readResponse.getConfidence(), adResultResponse1_1.getConfidence(), 0.001);
        assertEquals(readResponse.getAnomalyScore(), adResultResponse1_1.getAnomalyScore(), 0.001);
        assertEquals(1, readResponse.getFeatures().size());
        assertEquals(readResponse.getFeatures().get(0), adResultResponse1_1.getFeatures().get(0));
        assertEquals(readResponse.getError(), adResultResponse1_1.getError());
    }

    public void testSerializeAnomalyResultResponse1_0() throws IOException {
        setUpAnomalyResultResponse();

        adResultResponse1_1.writeTo(output1_0);

        StreamInput streamInput = output1_0.bytes().streamInput();
        streamInput.setVersion(Version.V_1_0_0);
        AnomalyResultResponse1_0 readResponse = new AnomalyResultResponse1_0(streamInput);
        assertEquals(readResponse.getAnomalyGrade(), adResultResponse1_1.getAnomalyGrade(), 0.001);
        assertEquals(readResponse.getConfidence(), adResultResponse1_1.getConfidence(), 0.001);
        assertEquals(readResponse.getAnomalyScore(), adResultResponse1_1.getAnomalyScore(), 0.001);
        assertEquals(1, readResponse.getFeatures().size());
        assertEquals(readResponse.getFeatures().get(0), adResultResponse1_1.getFeatures().get(0));
        assertEquals(readResponse.getError(), adResultResponse1_1.getError());
    }
}
