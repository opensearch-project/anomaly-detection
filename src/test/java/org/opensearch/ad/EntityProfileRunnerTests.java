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

package org.opensearch.ad;

import static java.util.Collections.emptyMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.TotalHits;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.EntityProfile;
import org.opensearch.ad.model.EntityProfileName;
import org.opensearch.ad.model.EntityState;
import org.opensearch.ad.model.InitProgressProfile;
import org.opensearch.ad.model.ModelProfile;
import org.opensearch.ad.model.ModelProfileOnNode;
import org.opensearch.ad.transport.EntityProfileAction;
import org.opensearch.ad.transport.EntityProfileResponse;
import org.opensearch.ad.util.SecurityClientUtil;
import org.opensearch.client.Client;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;

public class EntityProfileRunnerTests extends AbstractTimeSeriesTest {
    private AnomalyDetector detector;
    private int detectorIntervalMin;
    private Client client;
    private SecurityClientUtil clientUtil;
    private EntityProfileRunner runner;
    private Set<EntityProfileName> state;
    private Set<EntityProfileName> initNInfo;
    private Set<EntityProfileName> model;
    private String detectorId;
    private String entityValue;
    private int requiredSamples;
    private AnomalyDetectorJob job;

    private int smallUpdates;
    private String categoryField;
    private long latestSampleTimestamp;
    private long latestActiveTimestamp;
    private Boolean isActive;
    private String modelId;
    private long modelSize;
    private String nodeId;
    private Entity entity;

    enum InittedEverResultStatus {
        UNKNOWN,
        INITTED,
        NOT_INITTED,
    }

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyDetectorJobRunnerTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        detectorIntervalMin = 3;

        state = new HashSet<EntityProfileName>();
        state.add(EntityProfileName.STATE);

        initNInfo = new HashSet<EntityProfileName>();
        initNInfo.add(EntityProfileName.INIT_PROGRESS);
        initNInfo.add(EntityProfileName.ENTITY_INFO);

        model = new HashSet<EntityProfileName>();
        model.add(EntityProfileName.MODELS);

        detectorId = "A69pa3UBHuCbh-emo9oR";
        entityValue = "app-0";

        categoryField = "a";
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(categoryField));
        job = TestHelpers.randomAnomalyDetectorJob(true);

        requiredSamples = 128;
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(nodeStateManager).getAnomalyDetector(any(String.class), any(ActionListener.class));
        clientUtil = new SecurityClientUtil(nodeStateManager, Settings.EMPTY);

        runner = new EntityProfileRunner(client, clientUtil, xContentRegistry(), requiredSamples);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            String indexName = request.index();
            if (indexName.equals(CommonName.CONFIG_INDEX)) {
                listener.onResponse(TestHelpers.createGetResponse(detector, detector.getId(), CommonName.CONFIG_INDEX));
            } else if (indexName.equals(CommonName.JOB_INDEX)) {
                listener.onResponse(TestHelpers.createGetResponse(job, detector.getId(), CommonName.JOB_INDEX));
            }

            return null;
        }).when(client).get(any(), any());

        entity = Entity.createSingleAttributeEntity(categoryField, entityValue);
        modelId = entity.getModelId(detectorId).get();
    }

    @SuppressWarnings("unchecked")
    private void setUpSearch() {
        latestSampleTimestamp = 1_603_989_830_158L;
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            SearchRequest request = (SearchRequest) args[0];
            String indexName = request.indices()[0];
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            if (indexName.equals(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS)) {
                InternalMax maxAgg = new InternalMax(CommonName.AGG_NAME_MAX_TIME, latestSampleTimestamp, DocValueFormat.RAW, emptyMap());
                InternalAggregations internalAggregations = InternalAggregations.from(Collections.singletonList(maxAgg));

                SearchHits hits = new SearchHits(new SearchHit[] {}, null, Float.NaN);
                SearchResponseSections searchSections = new SearchResponseSections(hits, internalAggregations, null, false, false, null, 1);

                SearchResponse searchResponse = new SearchResponse(
                    searchSections,
                    null,
                    1,
                    1,
                    0,
                    30,
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                );

                listener.onResponse(searchResponse);
            }
            return null;
        }).when(client).search(any(), any());
    }

    @SuppressWarnings("unchecked")
    private void setUpExecuteEntityProfileAction(InittedEverResultStatus initted) {
        smallUpdates = 1;
        latestActiveTimestamp = 1603999189758L;
        isActive = Boolean.TRUE;
        modelId = "T4c3dXUBj-2IZN7itix__entity_" + entityValue;
        modelSize = 712480L;
        nodeId = "g6pmr547QR-CfpEvO67M4g";
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<EntityProfileResponse> listener = (ActionListener<EntityProfileResponse>) args[2];

            EntityProfileResponse.Builder profileResponseBuilder = new EntityProfileResponse.Builder();
            if (InittedEverResultStatus.UNKNOWN == initted) {
                profileResponseBuilder.setTotalUpdates(0L);
            } else if (InittedEverResultStatus.NOT_INITTED == initted) {
                profileResponseBuilder.setTotalUpdates(smallUpdates);
                profileResponseBuilder.setLastActiveMs(latestActiveTimestamp);
                profileResponseBuilder.setActive(isActive);
            } else {
                profileResponseBuilder.setTotalUpdates(requiredSamples + 1);
                ModelProfileOnNode model = new ModelProfileOnNode(nodeId, new ModelProfile(modelId, entity, modelSize));
                profileResponseBuilder.setModelProfile(model);
            }

            listener.onResponse(profileResponseBuilder.build());

            return null;
        }).when(client).execute(any(EntityProfileAction.class), any(), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            SearchRequest request = (SearchRequest) args[0];
            String indexName = request.indices()[0];
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            SearchResponse searchResponse = null;
            if (indexName.equals(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS)) {
                InternalMax maxAgg = new InternalMax(CommonName.AGG_NAME_MAX_TIME, latestSampleTimestamp, DocValueFormat.RAW, emptyMap());
                InternalAggregations internalAggregations = InternalAggregations.from(Collections.singletonList(maxAgg));

                SearchHits hits = new SearchHits(new SearchHit[] {}, null, Float.NaN);
                SearchResponseSections searchSections = new SearchResponseSections(hits, internalAggregations, null, false, false, null, 1);

                searchResponse = new SearchResponse(
                    searchSections,
                    null,
                    1,
                    1,
                    0,
                    30,
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                );
            } else {
                SearchHits collapsedHits = new SearchHits(
                    new SearchHit[] {
                        new SearchHit(2, "ID", Collections.emptyMap(), Collections.emptyMap()),
                        new SearchHit(3, "ID", Collections.emptyMap(), Collections.emptyMap()) },
                    new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                    1.0F
                );

                InternalSearchResponse internalSearchResponse = new InternalSearchResponse(collapsedHits, null, null, null, false, null, 1);
                searchResponse = new SearchResponse(
                    internalSearchResponse,
                    null,
                    1,
                    1,
                    0,
                    0,
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                );
            }

            listener.onResponse(searchResponse);

            return null;

        }).when(client).search(any(), any());
    }

    public void stateTestTemplate(InittedEverResultStatus returnedState, EntityState expectedState) throws InterruptedException {
        setUpExecuteEntityProfileAction(returnedState);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detectorId, entity, state, ActionListener.wrap(response -> {
            assertEquals(expectedState, response.getState());
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testRunningState() throws InterruptedException {
        stateTestTemplate(InittedEverResultStatus.INITTED, EntityState.RUNNING);
    }

    public void testUnknownState() throws InterruptedException {
        stateTestTemplate(InittedEverResultStatus.UNKNOWN, EntityState.UNKNOWN);
    }

    public void testInitState() throws InterruptedException {
        stateTestTemplate(InittedEverResultStatus.NOT_INITTED, EntityState.INIT);
    }

    public void testEmptyProfile() throws InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detectorId, entity, new HashSet<>(), ActionListener.wrap(response -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(exception.getMessage().contains(ADCommonMessages.EMPTY_PROFILES_COLLECT));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testModel() throws InterruptedException {
        setUpExecuteEntityProfileAction(InittedEverResultStatus.INITTED);
        EntityProfile.Builder expectedProfile = new EntityProfile.Builder();

        ModelProfileOnNode modelProfile = new ModelProfileOnNode(nodeId, new ModelProfile(modelId, entity, modelSize));
        expectedProfile.modelProfile(modelProfile);
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        runner.profile(detectorId, entity, model, ActionListener.wrap(response -> {
            assertEquals(expectedProfile.build(), response);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testEmptyModelProfile() throws IOException {
        ModelProfile modelProfile = new ModelProfile(modelId, null, modelSize);
        BytesStreamOutput output = new BytesStreamOutput();
        modelProfile.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        ModelProfile readResponse = new ModelProfile(streamInput);
        assertEquals("serialization has the wrong model id", modelId, readResponse.getModelId());
        assertTrue("serialization has null entity", null == readResponse.getEntity());
        assertEquals("serialization has the wrong model size", modelSize, readResponse.getModelSizeInBytes());

    }

    @SuppressWarnings("unchecked")
    public void testJobIndexNotFound() throws InterruptedException {
        setUpExecuteEntityProfileAction(InittedEverResultStatus.INITTED);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            String indexName = request.index();
            if (indexName.equals(CommonName.CONFIG_INDEX)) {
                listener.onResponse(TestHelpers.createGetResponse(detector, detector.getId(), CommonName.CONFIG_INDEX));
            } else if (indexName.equals(CommonName.JOB_INDEX)) {
                listener.onFailure(new IndexNotFoundException(CommonName.JOB_INDEX));
            }

            return null;
        }).when(client).get(any(), any());

        EntityProfile expectedProfile = new EntityProfile.Builder().build();

        runner.profile(detectorId, entity, initNInfo, ActionListener.wrap(response -> {
            assertEquals(expectedProfile, response);
            inProgressLatch.countDown();
        }, exception -> {
            LOG.error("Unexpected error", exception);
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unchecked")
    public void testNotMultiEntityDetector() throws IOException, InterruptedException {
        detector = TestHelpers.randomAnomalyDetectorWithInterval(new IntervalTimeConfiguration(detectorIntervalMin, ChronoUnit.MINUTES));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            String indexName = request.index();
            if (indexName.equals(CommonName.CONFIG_INDEX)) {
                listener.onResponse(TestHelpers.createGetResponse(detector, detector.getId(), CommonName.CONFIG_INDEX));
            }

            return null;
        }).when(client).get(any(), any());

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detectorId, entity, state, ActionListener.wrap(response -> {
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }, exception -> {
            assertTrue(exception.getMessage().contains(EntityProfileRunner.NOT_HC_DETECTOR_ERR_MSG));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    public void testInitNInfo() throws InterruptedException {
        setUpExecuteEntityProfileAction(InittedEverResultStatus.NOT_INITTED);
        latestSampleTimestamp = 1_603_989_830_158L;

        EntityProfile.Builder expectedProfile = new EntityProfile.Builder();

        // 1 / 128 rounded to 1%
        int neededSamples = requiredSamples - smallUpdates;
        InitProgressProfile profile = new InitProgressProfile("1%", neededSamples * detector.getIntervalInSeconds() / 60, neededSamples);
        expectedProfile.initProgress(profile);
        expectedProfile.isActive(isActive);
        expectedProfile.lastActiveTimestampMs(latestActiveTimestamp);
        expectedProfile.lastSampleTimestampMs(latestSampleTimestamp);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        runner.profile(detectorId, entity, initNInfo, ActionListener.wrap(response -> {
            assertEquals(expectedProfile.build(), response);
            inProgressLatch.countDown();
        }, exception -> {
            LOG.error("Unexpected error", exception);
            assertTrue("Should not reach here", false);
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }
}
