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

/*
package org.opensearch.ad;


public class EntityProfileRunnerTests extends AbstractADTest {
    private AnomalyDetector detector;
    private int detectorIntervalMin;
    private Client client;
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

    @SuppressWarnings("unchecked")
    @Override
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

        requiredSamples = 128;
        client = mock(Client.class);

        runner = new EntityProfileRunner(client, xContentRegistry(), requiredSamples);

        categoryField = "a";
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(categoryField));
        job = TestHelpers.randomAnomalyDetectorJob(true);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            String indexName = request.index();
            if (indexName.equals(ANOMALY_DETECTORS_INDEX)) {
                listener
                    .onResponse(TestHelpers.createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX));
            } else if (indexName.equals(ANOMALY_DETECTOR_JOB_INDEX)) {
                listener
                    .onResponse(
                        TestHelpers.createGetResponse(job, detector.getDetectorId(), AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)
                    );
            }

            return null;
        }).when(client).get(any(), any());

        entity = Entity.createSingleAttributeEntity(categoryField, entityValue);
    }

    @SuppressWarnings("unchecked")
    private void setUpSearch() {
        latestSampleTimestamp = 1_603_989_830_158L;
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            SearchRequest request = (SearchRequest) args[0];
            String indexName = request.indices()[0];
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            if (indexName.equals(CommonName.ANOMALY_RESULT_INDEX_ALIAS)) {
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
            if (indexName.equals(CommonName.ANOMALY_RESULT_INDEX_ALIAS)) {
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
            assertTrue(exception.getMessage().contains(CommonErrorMessages.EMPTY_PROFILES_COLLECT));
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

    @SuppressWarnings("unchecked")
    public void testJobIndexNotFound() throws InterruptedException {
        setUpExecuteEntityProfileAction(InittedEverResultStatus.INITTED);

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            String indexName = request.index();
            if (indexName.equals(ANOMALY_DETECTORS_INDEX)) {
                listener
                    .onResponse(TestHelpers.createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX));
            } else if (indexName.equals(ANOMALY_DETECTOR_JOB_INDEX)) {
                listener.onFailure(new IndexNotFoundException(ANOMALY_DETECTOR_JOB_INDEX));
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
            if (indexName.equals(ANOMALY_DETECTORS_INDEX)) {
                listener
                    .onResponse(TestHelpers.createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX));
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
        InitProgressProfile profile = new InitProgressProfile(
            "1%",
            neededSamples * detector.getDetectorIntervalInSeconds() / 60,
            neededSamples
        );
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
*/
