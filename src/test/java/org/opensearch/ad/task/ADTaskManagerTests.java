/* * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.*/

/*

package org.opensearch.ad.task;


public class ADTaskManagerTests extends ADUnitTestCase {

    private Settings settings;
    private Client client;
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;
    private DiscoveryNodeFilterer nodeFilter;
    private AnomalyDetectionIndices detectionIndices;
    private ADTaskCacheManager adTaskCacheManager;
    private HashRing hashRing;
    private TransportService transportService;
    private ADTaskManager adTaskManager;
    private ThreadPool threadPool;
    private IndexAnomalyDetectorJobActionHandler indexAnomalyDetectorJobActionHandler;

    private DetectionDateRange detectionDateRange;
    private ActionListener<AnomalyDetectorJobResponse> listener;

    private DiscoveryNode node1;
    private DiscoveryNode node2;

    private int maxRunningEntities;
    private int maxBatchTaskPerNode;

    private String historicalTaskId = "test_historical_task_id";
    private String realtimeTaskId = "test_realtime_task_id";
    private String runningHistoricalHCTaskContent = "{\"_index\":\".opendistro-anomaly-detection-state\",\"_type\":\"_doc\",\"_id\":\""
        + historicalTaskId
        + "\",\"_score\":1,\"_source\":{\"last_update_time\":1630999442827,\"state\":\"RUNNING\",\"detector_id\":"
        + "\"tQQiv3sBr1GKRuDiJ5uI\",\"task_progress\":1,\"init_progress\":1,\"execution_start_time\":1630999393798,"
        + "\"is_latest\":true,\"task_type\":\"HISTORICAL_HC_DETECTOR\",\"coordinating_node\":\"u8aYDPmaS4Ccd08Ed0GNQw\","
        + "\"detector\":{\"name\":\"test-hc1\",\"description\":\"test\",\"time_field\":\"timestamp\","
        + "\"indices\":[\"nab_ec2_cpu_utilization_24ae8d\"],\"filter_query\":{\"match_all\":{\"boost\":1}},"
        + "\"detection_interval\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\""
        + ":{\"interval\":1,\"unit\":\"Minutes\"}},\"shingle_size\":8,\"schema_version\":0,\"feature_attributes\""
        + ":[{\"feature_id\":\"tAQiv3sBr1GKRuDiJ5ty\",\"feature_name\":\"F1\",\"feature_enabled\":true,"
        + "\"aggregation_query\":{\"f_1\":{\"sum\":{\"field\":\"value\"}}}}],\"ui_metadata\":{\"features\":"
        + "{\"F1\":{\"featureType\":\"simple_aggs\",\"aggregationBy\":\"sum\",\"aggregationOf\":\"value\"}},"
        + "\"filters\":[]},\"last_update_time\":1630999291783,\"category_field\":[\"type\"],\"detector_type\":"
        + "\"MULTI_ENTITY\"},\"detection_date_range\":{\"start_time\":1628407291580,\"end_time\":1630999291580},"
        + "\"entity\":[{\"name\":\"type\",\"value\":\"error10\"}],\"parent_task_id\":\"a1civ3sBwF58XZxvKrko\","
        + "\"worker_node\":\"DL5uOJV3TjOOAyh5hJXrCA\",\"current_piece\":1630999260000,\"execution_end_time\":1630999442814}}";

    private String taskContent = "{\"_index\":\".opendistro-anomaly-detection-state\",\"_type\":\"_doc\",\"_id\":"
        + "\"-1ojv3sBwF58XZxvtksG\",\"_score\":1,\"_source\":{\"last_update_time\":1630999442827,\"state\":\"FINISHED\""
        + ",\"detector_id\":\"tQQiv3sBr1GKRuDiJ5uI\",\"task_progress\":1,\"init_progress\":1,\"execution_start_time\""
        + ":1630999393798,\"is_latest\":true,\"task_type\":\"HISTORICAL_HC_ENTITY\",\"coordinating_node\":\""
        + "u8aYDPmaS4Ccd08Ed0GNQw\",\"detector\":{\"name\":\"test-hc1\",\"description\":\"test\",\"time_field\":\""
        + "timestamp\",\"indices\":[\"nab_ec2_cpu_utilization_24ae8d\"],\"filter_query\":{\"match_all\":{\"boost\":1}}"
        + ",\"detection_interval\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":"
        + "{\"interval\":1,\"unit\":\"Minutes\"}},\"shingle_size\":8,\"schema_version\":0,\"feature_attributes\":"
        + "[{\"feature_id\":\"tAQiv3sBr1GKRuDiJ5ty\",\"feature_name\":\"F1\",\"feature_enabled\":true,\"aggregation_query"
        + "\":{\"f_1\":{\"sum\":{\"field\":\"value\"}}}}],\"ui_metadata\":{\"features\":{\"F1\":{\"featureType\":"
        + "\"simple_aggs\",\"aggregationBy\":\"sum\",\"aggregationOf\":\"value\"}},\"filters\":[]},\"last_update_time"
        + "\":1630999291783,\"category_field\":[\"type\"],\"detector_type\":\"MULTI_ENTITY\"},\"detection_date_range\""
        + ":{\"start_time\":1628407291580,\"end_time\":1630999291580},\"entity\":[{\"name\":\"type\",\"value\":\"error10\"}]"
        + ",\"parent_task_id\":\"a1civ3sBwF58XZxvKrko\",\"worker_node\":\"DL5uOJV3TjOOAyh5hJXrCA\",\"current_piece\""
        + ":1630999260000,\"execution_end_time\":1630999442814}}";
    @Captor
    ArgumentCaptor<TransportResponseHandler<AnomalyDetectorJobResponse>> remoteResponseHandler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Instant now = Instant.now();
        Instant startTime = now.minus(10, ChronoUnit.DAYS);
        Instant endTime = now.minus(1, ChronoUnit.DAYS);
        detectionDateRange = new DetectionDateRange(startTime, endTime);

        settings = Settings
            .builder()
            .put(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.getKey(), 2)
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(10))
            .build();

        clusterSettings = clusterSetting(
            settings,
            MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
            BATCH_TASK_PIECE_INTERVAL_SECONDS,
            REQUEST_TIMEOUT,
            DELETE_AD_RESULT_WHEN_DELETE_DETECTOR,
            MAX_BATCH_TASK_PER_NODE,
            MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS
        );

        maxBatchTaskPerNode = MAX_BATCH_TASK_PER_NODE.get(settings);
        clusterService = spy(new ClusterService(settings, clusterSettings, null));

        client = mock(Client.class);
        nodeFilter = mock(DiscoveryNodeFilterer.class);
        detectionIndices = mock(AnomalyDetectionIndices.class);
        adTaskCacheManager = mock(ADTaskCacheManager.class);
        hashRing = mock(HashRing.class);
        transportService = mock(TransportService.class);
        threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        indexAnomalyDetectorJobActionHandler = mock(IndexAnomalyDetectorJobActionHandler.class);
        adTaskManager = spy(
            new ADTaskManager(
                settings,
                clusterService,
                client,
                TestHelpers.xContentRegistry(),
                detectionIndices,
                nodeFilter,
                hashRing,
                adTaskCacheManager,
                threadPool
            )
        );

        listener = spy(new ActionListener<AnomalyDetectorJobResponse>() {
            @Override
            public void onResponse(AnomalyDetectorJobResponse bulkItemResponses) {}

            @Override
            public void onFailure(Exception e) {}
        });

        node1 = new DiscoveryNode(
            "nodeName1",
            "node1",
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        node2 = new DiscoveryNode(
            "nodeName2",
            "node2",
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        maxRunningEntities = MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS.get(settings).intValue();
    }

    private void setupGetDetector(AnomalyDetector detector) {
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener
                .onResponse(
                    new GetResponse(
                        new GetResult(
                            AnomalyDetector.ANOMALY_DETECTORS_INDEX,
                            detector.getDetectorId(),
                            UNASSIGNED_SEQ_NO,
                            0,
                            -1,
                            true,
                            BytesReference.bytes(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS)),
                            Collections.emptyMap(),
                            Collections.emptyMap()
                        )
                    )
                );
            return null;
        }).when(client).get(any(), any());
    }

    private void setupHashRingWithSameLocalADVersionNodes() {
        doAnswer(invocation -> {
            Consumer<DiscoveryNode[]> function = invocation.getArgument(0);
            function.accept(new DiscoveryNode[] { node1, node2 });
            return null;
        }).when(hashRing).getNodesWithSameLocalAdVersion(any(), any());
    }

    private void setupHashRingWithOwningNode() {
        doAnswer(invocation -> {
            Consumer<Optional<DiscoveryNode>> function = invocation.getArgument(1);
            function.accept(Optional.of(node1));
            return null;
        }).when(hashRing).buildAndGetOwningNodeWithSameLocalAdVersion(any(), any(), any());
    }

    public void testCreateTaskIndexNotAcknowledged() throws IOException {
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(false, false, ANOMALY_RESULT_INDEX_ALIAS));
            return null;
        }).when(detectionIndices).initDetectionStateIndex(any());
        doReturn(false).when(detectionIndices).doesDetectorStateIndexExist();
        AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)), randomAlphaOfLength(5), 1, randomAlphaOfLength(5));
        setupGetDetector(detector);

        adTaskManager.startDetector(detector, detectionDateRange, randomUser(), transportService, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        String error = String.format(Locale.ROOT, CREATE_INDEX_NOT_ACKNOWLEDGED, DETECTION_STATE_INDEX);
        assertEquals(error, exceptionCaptor.getValue().getMessage());
    }

    public void testCreateTaskIndexWithResourceAlreadyExistsException() throws IOException {
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new ResourceAlreadyExistsException("index created"));
            return null;
        }).when(detectionIndices).initDetectionStateIndex(any());
        doReturn(false).when(detectionIndices).doesDetectorStateIndexExist();
        AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)), randomAlphaOfLength(5), 1, randomAlphaOfLength(5));
        setupGetDetector(detector);

        adTaskManager.startDetector(detector, detectionDateRange, randomUser(), transportService, listener);
        verify(listener, never()).onFailure(any());
    }

    public void testCreateTaskIndexWithException() throws IOException {
        String error = randomAlphaOfLength(5);
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new RuntimeException(error));
            return null;
        }).when(detectionIndices).initDetectionStateIndex(any());
        doReturn(false).when(detectionIndices).doesDetectorStateIndexExist();
        AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)), randomAlphaOfLength(5), 1, randomAlphaOfLength(5));
        setupGetDetector(detector);

        adTaskManager.startDetector(detector, detectionDateRange, randomUser(), transportService, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(error, exceptionCaptor.getValue().getMessage());
    }

    public void testStartDetectorWithNoEnabledFeature() throws IOException {
        AnomalyDetector detector = randomDetector(
            ImmutableList.of(randomFeature(false)),
            randomAlphaOfLength(5),
            1,
            randomAlphaOfLength(5)
        );
        setupGetDetector(detector);

        adTaskManager
            .startDetector(
                detector.getDetectorId(),
                detectionDateRange,
                indexAnomalyDetectorJobActionHandler,
                randomUser(),
                transportService,
                listener
            );
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
    }

    @SuppressWarnings("unchecked")
    public void testStartDetectorForHistoricalAnalysis() throws IOException {
        AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)), randomAlphaOfLength(5), 1, randomAlphaOfLength(5));
        setupGetDetector(detector);
        setupHashRingWithOwningNode();

        adTaskManager
            .startDetector(
                detector.getDetectorId(),
                detectionDateRange,
                indexAnomalyDetectorJobActionHandler,
                randomUser(),
                transportService,
                listener
            );
        verify(adTaskManager, times(1)).forwardRequestToLeadNode(any(), any(), any());
    }

    private void setupTaskSlots(int node1UsedTaskSlots, int node1AssignedTaskSLots, int node2UsedTaskSlots, int node2AssignedTaskSLots) {
        doAnswer(invocation -> {
            ActionListener<ADStatsNodesResponse> listener = invocation.getArgument(2);
            listener
                .onResponse(
                    new ADStatsNodesResponse(
                        new ClusterName(randomAlphaOfLength(5)),
                        ImmutableList
                            .of(
                                new ADStatsNodeResponse(
                                    node1,
                                    ImmutableMap
                                        .of(
                                            InternalStatNames.AD_USED_BATCH_TASK_SLOT_COUNT.getName(),
                                            node1UsedTaskSlots,
                                            InternalStatNames.AD_DETECTOR_ASSIGNED_BATCH_TASK_SLOT_COUNT.getName(),
                                            node1AssignedTaskSLots
                                        )
                                ),
                                new ADStatsNodeResponse(
                                    node2,
                                    ImmutableMap
                                        .of(
                                            InternalStatNames.AD_USED_BATCH_TASK_SLOT_COUNT.getName(),
                                            node2UsedTaskSlots,
                                            InternalStatNames.AD_DETECTOR_ASSIGNED_BATCH_TASK_SLOT_COUNT.getName(),
                                            node2AssignedTaskSLots
                                        )
                                )
                            ),
                        ImmutableList.of()
                    )
                );
            return null;
        }).when(client).execute(any(), any(), any());
    }

    public void testCheckTaskSlotsWithNoAvailableTaskSlots() throws IOException {
        ADTask adTask = randomAdTask(
            randomAlphaOfLength(5),
            ADTaskState.INIT,
            Instant.now(),
            randomAlphaOfLength(5),
            TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of(randomAlphaOfLength(5)))
        );
        setupHashRingWithSameLocalADVersionNodes();

        setupTaskSlots(0, maxBatchTaskPerNode, maxBatchTaskPerNode, maxBatchTaskPerNode);

        adTaskManager
            .checkTaskSlots(adTask, adTask.getDetector(), detectionDateRange, randomUser(), ADTaskAction.START, transportService, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains("No available task slot"));
    }

    private void setupSearchTopEntities(int entitySize) {
        List<Entity> entities = new ArrayList<>();
        for (int i = 0; i < entitySize; i++) {
            entities.add(createSingleAttributeEntity("category", "value" + i));
        }
    }

    public void testCheckTaskSlotsWithAvailableTaskSlotsForHC() throws IOException {
        ADTask adTask = randomAdTask(
            randomAlphaOfLength(5),
            ADTaskState.INIT,
            Instant.now(),
            randomAlphaOfLength(5),
            TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of(randomAlphaOfLength(5)))
        );
        setupSearchTopEntities(4);
        setupHashRingWithSameLocalADVersionNodes();

        setupTaskSlots(0, maxBatchTaskPerNode, maxBatchTaskPerNode, maxBatchTaskPerNode - 1);

        adTaskManager
            .checkTaskSlots(adTask, adTask.getDetector(), detectionDateRange, randomUser(), ADTaskAction.START, transportService, listener);
        verify(adTaskManager, times(1))
            .startHistoricalAnalysis(eq(adTask.getDetector()), eq(detectionDateRange), any(), eq(1), eq(transportService), any());
    }

    public void testCheckTaskSlotsWithAvailableTaskSlotsForSingleEntityDetector() throws IOException {
        ADTask adTask = randomAdTask(
            randomAlphaOfLength(5),
            ADTaskState.INIT,
            Instant.now(),
            randomAlphaOfLength(5),
            TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of())
        );
        setupHashRingWithSameLocalADVersionNodes();

        setupTaskSlots(0, 2, 2, 1);

        adTaskManager
            .checkTaskSlots(adTask, adTask.getDetector(), detectionDateRange, randomUser(), ADTaskAction.START, transportService, listener);
        verify(adTaskManager, times(1))
            .startHistoricalAnalysis(eq(adTask.getDetector()), eq(detectionDateRange), any(), eq(1), eq(transportService), any());
    }

    public void testCheckTaskSlotsWithAvailableTaskSlotsAndNoEntity() throws IOException {
        ADTask adTask = randomAdTask(
            randomAlphaOfLength(5),
            ADTaskState.INIT,
            Instant.now(),
            randomAlphaOfLength(5),
            TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of(randomAlphaOfLength(5)))
        );
        setupSearchTopEntities(0);
        setupHashRingWithSameLocalADVersionNodes();

        setupTaskSlots(0, 2, 2, 1);

        adTaskManager
            .checkTaskSlots(adTask, adTask.getDetector(), detectionDateRange, randomUser(), ADTaskAction.START, transportService, listener);
        verify(adTaskManager, times(1)).startHistoricalAnalysis(any(), any(), any(), anyInt(), any(), any());
    }

    public void testCheckTaskSlotsWithAvailableTaskSlotsForScale() throws IOException {
        ADTask adTask = randomAdTask(
            randomAlphaOfLength(5),
            ADTaskState.INIT,
            Instant.now(),
            randomAlphaOfLength(5),
            TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of(randomAlphaOfLength(5)))
        );
        setupSearchTopEntities(4);
        setupHashRingWithSameLocalADVersionNodes();

        setupTaskSlots(0, maxBatchTaskPerNode, maxBatchTaskPerNode, maxBatchTaskPerNode - 1);

        adTaskManager
            .checkTaskSlots(
                adTask,
                adTask.getDetector(),
                detectionDateRange,
                randomUser(),
                ADTaskAction.SCALE_ENTITY_TASK_SLOTS,
                transportService,
                listener
            );
        verify(adTaskManager, times(1)).scaleTaskLaneOnCoordinatingNode(eq(adTask), eq(1), eq(transportService), any());
    }

    public void testDeleteDuplicateTasks() throws IOException {
        ADTask adTask = randomAdTask();
        adTaskManager.handleADTaskException(adTask, new DuplicateTaskException("test"));
        verify(client, times(1)).delete(any(), any());
    }

    public void testParseEntityForSingleCategoryHC() throws IOException {
        ADTask adTask = randomAdTask(
            randomAlphaOfLength(5),
            ADTaskState.INIT,
            Instant.now(),
            randomAlphaOfLength(5),
            TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of(randomAlphaOfLength(5)))
        );
        String entityValue = adTaskManager.convertEntityToString(adTask);
        Entity entity = adTaskManager.parseEntityFromString(entityValue, adTask);
        assertEquals(entity, adTask.getEntity());
    }

    public void testParseEntityForMultiCategoryHC() throws IOException {
        ADTask adTask = randomAdTask(
            randomAlphaOfLength(5),
            ADTaskState.INIT,
            Instant.now(),
            randomAlphaOfLength(5),
            TestHelpers
                .randomAnomalyDetectorUsingCategoryFields(
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5), randomAlphaOfLength(5))
                )
        );
        String entityValue = adTaskManager.convertEntityToString(adTask);
        Entity entity = adTaskManager.parseEntityFromString(entityValue, adTask);
        assertEquals(entity, adTask.getEntity());
    }

    public void testDetectorTaskSlotScaleUpDelta() {
        String detectorId = randomAlphaOfLength(5);
        DiscoveryNode[] eligibleDataNodes = new DiscoveryNode[] { node1, node2 };

        // Scale down
        when(hashRing.getNodesWithSameLocalAdVersion()).thenReturn(eligibleDataNodes);
        when(adTaskCacheManager.getUnfinishedEntityCount(detectorId)).thenReturn(maxRunningEntities * 10);
        int taskSlots = maxRunningEntities - 1;
        when(adTaskCacheManager.getDetectorTaskSlots(detectorId)).thenReturn(taskSlots);
        int delta = adTaskManager.detectorTaskSlotScaleDelta(detectorId);
        assertEquals(maxRunningEntities - taskSlots, delta);
    }

    public void testDetectorTaskSlotScaleDownDelta() {
        String detectorId = randomAlphaOfLength(5);
        DiscoveryNode[] eligibleDataNodes = new DiscoveryNode[] { node1, node2 };

        // Scale down
        when(hashRing.getNodesWithSameLocalAdVersion()).thenReturn(eligibleDataNodes);
        when(adTaskCacheManager.getUnfinishedEntityCount(detectorId)).thenReturn(maxRunningEntities * 10);
        int taskSlots = maxRunningEntities * 5;
        when(adTaskCacheManager.getDetectorTaskSlots(detectorId)).thenReturn(taskSlots);
        int delta = adTaskManager.detectorTaskSlotScaleDelta(detectorId);
        assertEquals(maxRunningEntities - taskSlots, delta);
    }

    @SuppressWarnings("unchecked")
    public void testGetADTaskWithNullResponse() {
        String taskId = randomAlphaOfLength(5);
        ActionListener<Optional<ADTask>> actionListener = mock(ActionListener.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(null);
            return null;
        }).when(client).get(any(), any());

        adTaskManager.getADTask(taskId, actionListener);
        verify(actionListener, times(1)).onResponse(eq(Optional.empty()));
    }

@SuppressWarnings("unchecked")
    public void testGetADTaskWithNotExistTask() {
        String taskId = randomAlphaOfLength(5);
        ActionListener<Optional<ADTask>> actionListener = mock(ActionListener.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            GetResponse response = new GetResponse(
                new GetResult(
                    AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX,
                    taskId,
                    UNASSIGNED_SEQ_NO,
                    0,
                    -1,
                    false,
                    null,
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );
            listener.onResponse(response);
            return null;
        }).when(client).get(any(), any());

        adTaskManager.getADTask(taskId, actionListener);
        verify(actionListener, times(1)).onResponse(eq(Optional.empty()));
    }


    @SuppressWarnings("unchecked")
    public void testGetADTaskWithIndexNotFoundException() {
        String taskId = randomAlphaOfLength(5);
        ActionListener<Optional<ADTask>> actionListener = mock(ActionListener.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onFailure(new IndexNotFoundException("", ""));
            return null;
        }).when(client).get(any(), any());

        adTaskManager.getADTask(taskId, actionListener);
        verify(actionListener, times(1)).onResponse(eq(Optional.empty()));
    }

    @SuppressWarnings("unchecked")
    public void testGetADTaskWithIndexUnknownException() {
        String taskId = randomAlphaOfLength(5);
        ActionListener<Optional<ADTask>> actionListener = mock(ActionListener.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("test"));
            return null;
        }).when(client).get(any(), any());

        adTaskManager.getADTask(taskId, actionListener);
        verify(actionListener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testGetADTaskWithExistingTask() {
        String taskId = randomAlphaOfLength(5);
        ActionListener<Optional<ADTask>> actionListener = mock(ActionListener.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            ADTask adTask = randomAdTask();
            GetResponse response = new GetResponse(
                new GetResult(
                    AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX,
                    taskId,
                    UNASSIGNED_SEQ_NO,
                    0,
                    -1,
                    true,
                    BytesReference.bytes(adTask.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS)),
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );
            listener.onResponse(response);
            return null;
        }).when(client).get(any(), any());

        adTaskManager.getADTask(taskId, actionListener);
        verify(actionListener, times(1)).onResponse(any());
    }

    @SuppressWarnings("unchecked")
    public void testUpdateLatestRealtimeTaskOnCoordinatingNode() {
        String detectorId = randomAlphaOfLength(5);
        String state = ADTaskState.RUNNING.name();
        Long rcfTotalUpdates = randomLongBetween(200, 1000);
        Long detectorIntervalInMinutes = 1L;
        String error = randomAlphaOfLength(5);
        ActionListener<UpdateResponse> actionListener = mock(ActionListener.class);
        doReturn(node1).when(clusterService).localNode();
        when(adTaskCacheManager.isRealtimeTaskChanged(anyString(), anyString(), anyFloat(), anyString())).thenReturn(true);
        doAnswer(invocation -> {
            ActionListener<UpdateResponse> listener = invocation.getArgument(3);
            listener.onResponse(new UpdateResponse(ShardId.fromString("[test][1]"), "1", 0L, 1L, 1L, DocWriteResponse.Result.UPDATED));
            return null;
        }).when(adTaskManager).updateLatestADTask(anyString(), any(), anyMap(), any());
        adTaskManager
            .updateLatestRealtimeTaskOnCoordinatingNode(
                detectorId,
                state,
                rcfTotalUpdates,
                detectorIntervalInMinutes,
                error,
                actionListener
            );
        verify(actionListener, times(1)).onResponse(any());
    }

    public void testGetLocalADTaskProfilesByDetectorId() {
        doReturn(node1).when(clusterService).localNode();
        when(adTaskCacheManager.isHCTaskRunning(anyString())).thenReturn(true);
        when(adTaskCacheManager.isHCTaskCoordinatingNode(anyString())).thenReturn(true);
        List<String> tasksOfDetector = ImmutableList.of(randomAlphaOfLength(5));
        when(adTaskCacheManager.getTasksOfDetector(anyString())).thenReturn(tasksOfDetector);
        Deque<Map.Entry<Long, Optional<double[]>>> shingle = new LinkedBlockingDeque<>();
        when(adTaskCacheManager.getShingle(anyString())).thenReturn(shingle);
        ThresholdedRandomCutForest trcf = mock(ThresholdedRandomCutForest.class);
        when(adTaskCacheManager.getTRcfModel(anyString())).thenReturn(trcf);
        RandomCutForest rcf = mock(RandomCutForest.class);
        when(trcf.getForest()).thenReturn(rcf);
        when(rcf.getTotalUpdates()).thenReturn(randomLongBetween(100, 1000));
        when(adTaskCacheManager.isThresholdModelTrained(anyString())).thenReturn(true);
        when(adTaskCacheManager.getThresholdModelTrainingDataSize(anyString())).thenReturn(randomIntBetween(100, 1000));
        when(adTaskCacheManager.getModelSize(anyString())).thenReturn(randomLongBetween(100, 1000));
        Entity entity = createSingleAttributeEntity(randomAlphaOfLength(5), randomAlphaOfLength(5));
        when(adTaskCacheManager.getEntity(anyString())).thenReturn(entity);
        String detectorId = randomAlphaOfLength(5);

        ExecutorService executeService = mock(ExecutorService.class);
        when(threadPool.executor(anyString())).thenReturn(executeService);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executeService).execute(any());

        ADTaskProfile taskProfile = adTaskManager.getLocalADTaskProfilesByDetectorId(detectorId);
        assertEquals(1, taskProfile.getEntityTaskProfiles().size());
        verify(adTaskCacheManager, times(1)).cleanExpiredHCBatchTaskRunStates();
    }

    @SuppressWarnings("unchecked")
    public void testRemoveStaleRunningEntity() throws IOException {
        ActionListener<AnomalyDetectorJobResponse> actionListener = mock(ActionListener.class);
        ADTask adTask = randomAdTask();
        String entity = randomAlphaOfLength(5);
        ExecutorService executeService = mock(ExecutorService.class);
        when(threadPool.executor(anyString())).thenReturn(executeService);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executeService).execute(any());
        when(adTaskCacheManager.removeRunningEntity(anyString(), anyString())).thenReturn(true);
        when(adTaskCacheManager.getPendingEntityCount(anyString())).thenReturn(randomIntBetween(1, 10));
        adTaskManager.removeStaleRunningEntity(adTask, entity, transportService, actionListener);
        verify(adTaskManager, times(1)).runNextEntityForHCADHistorical(any(), any(), any());

        when(adTaskCacheManager.removeRunningEntity(anyString(), anyString())).thenReturn(false);
        when(adTaskCacheManager.hasEntity(anyString())).thenReturn(false);
        adTaskManager.removeStaleRunningEntity(adTask, entity, transportService, actionListener);
        verify(adTaskManager, times(1)).setHCDetectorTaskDone(any(), any(), any());

        when(adTaskCacheManager.hasEntity(anyString())).thenReturn(true);
        adTaskManager.removeStaleRunningEntity(adTask, entity, transportService, actionListener);
        verify(adTaskManager, times(1)).setHCDetectorTaskDone(any(), any(), any());
    }

    public void testResetLatestFlagAsFalse() throws IOException {
        List<ADTask> adTasks = new ArrayList<>();
        adTaskManager.resetLatestFlagAsFalse(adTasks);
        verify(client, never()).execute(any(), any(), any());

        ADTask adTask = randomAdTask();
        adTasks.add(adTask);
        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(2);
            BulkItemResponse[] responses = new BulkItemResponse[1];
            ShardId shardId = new ShardId(new Index("index_name", "uuid"), 0);
            responses[0] = new BulkItemResponse(
                0,
                randomFrom(DocWriteRequest.OpType.values()),
                new IndexResponse(shardId, "id", 1, 1, 1, true)
            );
            listener.onResponse(new BulkResponse(responses, 1));
            return null;
        }).when(client).execute(any(), any(), any());
        adTaskManager.resetLatestFlagAsFalse(adTasks);
        verify(client, times(1)).execute(any(), any(), any());
    }

    public void testCleanADResultOfDeletedDetectorWithNoDeletedDetector() {
        when(adTaskCacheManager.pollDeletedDetector()).thenReturn(null);
        adTaskManager.cleanADResultOfDeletedDetector();
        verify(client, never()).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());
    }

    public void testCleanADResultOfDeletedDetectorWithException() {
        String detectorId = randomAlphaOfLength(5);
        when(adTaskCacheManager.pollDeletedDetector()).thenReturn(detectorId);

        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException("test"));
            return null;
        }).doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> listener = invocation.getArgument(2);
            BulkByScrollResponse deleteByQueryResponse = mock(BulkByScrollResponse.class);
            listener.onResponse(deleteByQueryResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        settings = Settings
            .builder()
            .put(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.getKey(), 2)
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(10))
            .put(DELETE_AD_RESULT_WHEN_DELETE_DETECTOR.getKey(), true)
            .build();

        clusterSettings = clusterSetting(
            settings,
            MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
            BATCH_TASK_PIECE_INTERVAL_SECONDS,
            REQUEST_TIMEOUT,
            DELETE_AD_RESULT_WHEN_DELETE_DETECTOR,
            MAX_BATCH_TASK_PER_NODE,
            MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS
        );

        clusterService = spy(new ClusterService(settings, clusterSettings, null));

        ADTaskManager adTaskManager = spy(
            new ADTaskManager(
                settings,
                clusterService,
                client,
                TestHelpers.xContentRegistry(),
                detectionIndices,
                nodeFilter,
                hashRing,
                adTaskCacheManager,
                threadPool
            )
        );
        adTaskManager.cleanADResultOfDeletedDetector();
        verify(client, times(1)).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());
        verify(adTaskCacheManager, times(1)).addDeletedDetector(eq(detectorId));

        adTaskManager.cleanADResultOfDeletedDetector();
        verify(client, times(2)).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());
        verify(adTaskCacheManager, times(1)).addDeletedDetector(eq(detectorId));
    }

    public void testMaintainRunningHistoricalTasksWithOwningNodeIsNotLocalNode() {
        // Test no owning node
        when(hashRing.getOwningNodeWithHighestAdVersion(anyString())).thenReturn(Optional.empty());
        adTaskManager.maintainRunningHistoricalTasks(transportService, 10);
        verify(client, never()).search(any(), any());

        // Test owning node is not local node
        when(hashRing.getOwningNodeWithHighestAdVersion(anyString())).thenReturn(Optional.of(node2));
        doReturn(node1).when(clusterService).localNode();
        adTaskManager.maintainRunningHistoricalTasks(transportService, 10);
        verify(client, never()).search(any(), any());
    }

    public void testMaintainRunningHistoricalTasksWithNoRunningTask() {
        when(hashRing.getOwningNodeWithHighestAdVersion(anyString())).thenReturn(Optional.of(node1));
        doReturn(node1).when(clusterService).localNode();

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
            InternalSearchResponse response = new InternalSearchResponse(
                searchHits,
                InternalAggregations.EMPTY,
                null,
                null,
                false,
                null,
                1
            );
            SearchResponse searchResponse = new SearchResponse(
                response,
                null,
                1,
                1,
                0,
                100,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());
        adTaskManager.maintainRunningHistoricalTasks(transportService, 10);
        verify(client, times(1)).search(any(), any());
    }

    public void testMaintainRunningHistoricalTasksWithRunningTask() {
        when(hashRing.getOwningNodeWithHighestAdVersion(anyString())).thenReturn(Optional.of(node1));
        doReturn(node1).when(clusterService).localNode();
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(threadPool).schedule(any(), any(), anyString());

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchHit task = SearchHit.fromXContent(TestHelpers.parser(runningHistoricalHCTaskContent));
            SearchHits searchHits = new SearchHits(new SearchHit[] { task }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), Float.NaN);
            InternalSearchResponse response = new InternalSearchResponse(
                searchHits,
                InternalAggregations.EMPTY,
                null,
                null,
                false,
                null,
                1
            );
            SearchResponse searchResponse = new SearchResponse(
                response,
                null,
                1,
                1,
                0,
                100,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());
        adTaskManager.maintainRunningHistoricalTasks(transportService, 10);
        verify(client, times(1)).search(any(), any());
    }

    public void testMaintainRunningRealtimeTasksWithNoRealtimeTask() {
        when(adTaskCacheManager.getDetectorIdsInRealtimeTaskCache()).thenReturn(null);
        adTaskManager.maintainRunningRealtimeTasks();
        verify(adTaskCacheManager, never()).removeRealtimeTaskCache(anyString());

        when(adTaskCacheManager.getDetectorIdsInRealtimeTaskCache()).thenReturn(new String[0]);
        adTaskManager.maintainRunningRealtimeTasks();
        verify(adTaskCacheManager, never()).removeRealtimeTaskCache(anyString());
    }

    public void testMaintainRunningRealtimeTasks() {
        String detectorId1 = randomAlphaOfLength(5);
        String detectorId2 = randomAlphaOfLength(5);
        String detectorId3 = randomAlphaOfLength(5);
        when(adTaskCacheManager.getDetectorIdsInRealtimeTaskCache()).thenReturn(new String[] { detectorId1, detectorId2, detectorId3 });
        when(adTaskCacheManager.getRealtimeTaskCache(detectorId1)).thenReturn(null);

        ADRealtimeTaskCache cacheOfDetector2 = mock(ADRealtimeTaskCache.class);
        when(cacheOfDetector2.expired()).thenReturn(false);
        when(adTaskCacheManager.getRealtimeTaskCache(detectorId2)).thenReturn(cacheOfDetector2);

        ADRealtimeTaskCache cacheOfDetector3 = mock(ADRealtimeTaskCache.class);
        when(cacheOfDetector3.expired()).thenReturn(true);
        when(adTaskCacheManager.getRealtimeTaskCache(detectorId3)).thenReturn(cacheOfDetector3);

        adTaskManager.maintainRunningRealtimeTasks();
        verify(adTaskCacheManager, times(1)).removeRealtimeTaskCache(anyString());
    }

    @SuppressWarnings("unchecked")
    public void testStartHistoricalAnalysisWithNoOwningNode() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of());
        DetectionDateRange detectionDateRange = TestHelpers.randomDetectionDateRange();
        UserIdentity user = null;
        int availableTaskSlots = randomIntBetween(1, 10);
        ActionListener<AnomalyDetectorJobResponse> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            Consumer<Optional<DiscoveryNode>> function = invocation.getArgument(1);
            function.accept(Optional.empty());
            return null;
        }).when(hashRing).buildAndGetOwningNodeWithSameLocalAdVersion(anyString(), any(), any());
        adTaskManager.startHistoricalAnalysis(detector, detectionDateRange, user, availableTaskSlots, transportService, listener);
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testGetAndExecuteOnLatestADTasksWithRunningRealtimeTaskWithTaskStopped() throws IOException {
        String detectorId = randomAlphaOfLength(5);
        Consumer<List<ADTask>> function = mock(Consumer.class);
        AnomalyDetector detector = TestHelpers
            .randomDetector(
                ImmutableList.of(randomFeature(true)),
                randomAlphaOfLength(5),
                randomIntBetween(1, 10),
                MockSimpleLog.TIME_FIELD,
                ImmutableList.of(randomAlphaOfLength(5))
            );
        ADTask adTask = ADTask
            .builder()
            .taskId(randomAlphaOfLength(5))
            .taskType(ADTaskType.HISTORICAL_HC_DETECTOR.name())
            .detectorId(randomAlphaOfLength(5))
            .detector(detector)
            .entity(null)
            .state(ADTaskState.RUNNING.name())
            .taskProgress(0.5f)
            .initProgress(1.0f)
            .currentPiece(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(randomIntBetween(1, 100), ChronoUnit.MINUTES))
            .executionStartTime(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(100, ChronoUnit.MINUTES))
            .isLatest(true)
            .error(randomAlphaOfLength(5))
            .checkpointId(randomAlphaOfLength(5))
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .startedBy(randomAlphaOfLength(5))
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .coordinatingNode(node1.getId())
            .build();
        ADTaskProfile profile = new ADTaskProfile(
            adTask,
            randomInt(),
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
        setupGetAndExecuteOnLatestADTasks(profile);
        adTaskManager
            .getAndExecuteOnLatestADTasks(
                detectorId,
                null,
                null,
                ADTaskType.ALL_DETECTOR_TASK_TYPES,
                function,
                transportService,
                true,
                10,
                listener
            );
        verify(client, times(2)).update(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testGetAndExecuteOnLatestADTasksWithRunningHistoricalTask() throws IOException {
        String detectorId = randomAlphaOfLength(5);
        Consumer<List<ADTask>> function = mock(Consumer.class);
        AnomalyDetector detector = TestHelpers
            .randomDetector(
                ImmutableList.of(randomFeature(true)),
                randomAlphaOfLength(5),
                randomIntBetween(1, 10),
                MockSimpleLog.TIME_FIELD,
                ImmutableList.of(randomAlphaOfLength(5))
            );
        ADTask adTask = ADTask
            .builder()
            .taskId(historicalTaskId)
            .taskType(ADTaskType.HISTORICAL_HC_DETECTOR.name())
            .detectorId(randomAlphaOfLength(5))
            .detector(detector)
            .entity(null)
            .state(ADTaskState.RUNNING.name())
            .taskProgress(0.5f)
            .initProgress(1.0f)
            .currentPiece(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(randomIntBetween(1, 100), ChronoUnit.MINUTES))
            .executionStartTime(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(100, ChronoUnit.MINUTES))
            .isLatest(true)
            .error(randomAlphaOfLength(5))
            .checkpointId(randomAlphaOfLength(5))
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .startedBy(randomAlphaOfLength(5))
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .coordinatingNode(node1.getId())
            .build();
        ADTaskProfile profile = new ADTaskProfile(
            adTask,
            randomInt(),
            randomLong(),
            randomBoolean(),
            randomInt(),
            randomLong(),
            randomAlphaOfLength(5),
            historicalTaskId,
            randomAlphaOfLength(5),
            randomInt(),
            randomBoolean(),
            randomInt(),
            randomInt(),
            2,
            ImmutableList.of(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            Instant.now().toEpochMilli()
        );
        setupGetAndExecuteOnLatestADTasks(profile);
        adTaskManager
            .getAndExecuteOnLatestADTasks(
                detectorId,
                null,
                null,
                ADTaskType.ALL_DETECTOR_TASK_TYPES,
                function,
                transportService,
                true,
                10,
                listener
            );
        verify(client, times(2)).update(any(), any());
    }

    @SuppressWarnings("unchecked")
    private void setupGetAndExecuteOnLatestADTasks(ADTaskProfile adTaskProfile) {
        String runningRealtimeHCTaskContent = runningHistoricalHCTaskContent
            .replace(ADTaskType.HISTORICAL_HC_DETECTOR.name(), ADTaskType.REALTIME_HC_DETECTOR.name())
            .replace(historicalTaskId, realtimeTaskId);

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchHit historicalTask = SearchHit.fromXContent(TestHelpers.parser(runningHistoricalHCTaskContent));
            SearchHit realtimeTask = SearchHit.fromXContent(TestHelpers.parser(runningRealtimeHCTaskContent));
            SearchHits searchHits = new SearchHits(
                new SearchHit[] { historicalTask, realtimeTask },
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                Float.NaN
            );
            InternalSearchResponse response = new InternalSearchResponse(
                searchHits,
                InternalAggregations.EMPTY,
                null,
                null,
                false,
                null,
                1
            );
            SearchResponse searchResponse = new SearchResponse(
                response,
                null,
                1,
                1,
                0,
                100,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());
        String detectorId = randomAlphaOfLength(5);
        Consumer<List<ADTask>> function = mock(Consumer.class);
        ActionListener<AnomalyDetectorJobResponse> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            Consumer<DiscoveryNode[]> getNodeFunction = invocation.getArgument(0);
            getNodeFunction.accept(new DiscoveryNode[] { node1, node2 });
            return null;
        }).when(hashRing).getAllEligibleDataNodesWithKnownAdVersion(any(), any());

        doAnswer(invocation -> {
            ActionListener<ADTaskProfileResponse> taskProfileResponseListener = invocation.getArgument(2);
            AnomalyDetector detector = TestHelpers
                .randomDetector(
                    ImmutableList.of(randomFeature(true)),
                    randomAlphaOfLength(5),
                    randomIntBetween(1, 10),
                    MockSimpleLog.TIME_FIELD,
                    ImmutableList.of(randomAlphaOfLength(5))
                );
            ADTaskProfileNodeResponse nodeResponse = new ADTaskProfileNodeResponse(node1, adTaskProfile, Version.CURRENT);
            ImmutableList<ADTaskProfileNodeResponse> nodes = ImmutableList.of(nodeResponse);
            ADTaskProfileResponse taskProfileResponse = new ADTaskProfileResponse(new ClusterName("test"), nodes, ImmutableList.of());
            taskProfileResponseListener.onResponse(taskProfileResponse);
            return null;
        }).doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> updateResponselistener = invocation.getArgument(2);
            BulkByScrollResponse response = mock(BulkByScrollResponse.class);
            when(response.getBulkFailures()).thenReturn(null);
            updateResponselistener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());
        when(nodeFilter.getEligibleDataNodes()).thenReturn(new DiscoveryNode[] { node1, node2 });

        doAnswer(invocation -> {
            ActionListener<UpdateResponse> updateResponselistener = invocation.getArgument(1);
            UpdateResponse response = new UpdateResponse(ShardId.fromString("[test][1]"), "1", 0L, 1L, 1L, DocWriteResponse.Result.UPDATED);
            updateResponselistener.onResponse(response);
            return null;
        }).when(client).update(any(), any());

        doAnswer(invocation -> {
            ActionListener<GetResponse> getResponselistener = invocation.getArgument(1);
            GetResponse response = new GetResponse(
                new GetResult(
                    AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX,
                    detectorId,
                    UNASSIGNED_SEQ_NO,
                    0,
                    -1,
                    true,
                    BytesReference
                        .bytes(
                            new AnomalyDetectorJob(
                                detectorId,
                                randomIntervalSchedule(),
                                randomIntervalTimeConfiguration(),
                                false,
                                Instant.now().minusSeconds(60),
                                Instant.now(),
                                Instant.now(),
                                60L,
                                TestHelpers.randomUser(),
                                null
                            ).toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS)
                        ),
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );
            getResponselistener.onResponse(response);
            return null;
        }).when(client).get(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testCreateADTaskDirectlyWithException() throws IOException {
        ADTask adTask = randomAdTask(ADTaskType.HISTORICAL_HC_DETECTOR);
        Consumer<IndexResponse> function = mock(Consumer.class);
        ActionListener<IndexResponse> listener = mock(ActionListener.class);
        doThrow(new RuntimeException("test")).when(client).index(any(), any());

        adTaskManager.createADTaskDirectly(adTask, function, listener);
        verify(listener, times(1)).onFailure(any());

        doAnswer(invocation -> {
            ActionListener<IndexResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new RuntimeException("test"));
            return null;
        }).when(client).index(any(), any());
        adTaskManager.createADTaskDirectly(adTask, function, listener);
        verify(listener, times(2)).onFailure(any());
    }

    public void testCleanChildTasksAndADResultsOfDeletedTaskWithNoDeletedDetectorTask() {
        when(adTaskCacheManager.hasDeletedDetectorTask()).thenReturn(false);
        adTaskManager.cleanChildTasksAndADResultsOfDeletedTask();
        verify(client, never()).execute(any(), any(), any());
    }

    public void testCleanChildTasksAndADResultsOfDeletedTaskWithNullTask() {
        when(adTaskCacheManager.hasDeletedDetectorTask()).thenReturn(true);
        when(adTaskCacheManager.pollDeletedDetectorTask()).thenReturn(null);
        doAnswer(invocation -> {
            ActionListener<IndexResponse> actionListener = invocation.getArgument(2);
            actionListener.onFailure(new RuntimeException("test"));
            return null;
        }).when(client).execute(any(), any(), any());

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(threadPool).schedule(any(), any(), any());

        adTaskManager.cleanChildTasksAndADResultsOfDeletedTask();
        verify(client, never()).execute(any(), any(), any());
    }

    public void testCleanChildTasksAndADResultsOfDeletedTaskWithFailToDeleteADResult() {
        when(adTaskCacheManager.hasDeletedDetectorTask()).thenReturn(true);
        when(adTaskCacheManager.pollDeletedDetectorTask()).thenReturn(randomAlphaOfLength(5));
        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> actionListener = invocation.getArgument(2);
            actionListener.onFailure(new RuntimeException("test"));
            return null;
        }).when(client).execute(any(), any(), any());

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(threadPool).schedule(any(), any(), any());

        adTaskManager.cleanChildTasksAndADResultsOfDeletedTask();
        verify(client, times(1)).execute(any(), any(), any());
    }

    public void testCleanChildTasksAndADResultsOfDeletedTask() {
        when(adTaskCacheManager.hasDeletedDetectorTask()).thenReturn(true);
        when(adTaskCacheManager.pollDeletedDetectorTask()).thenReturn(randomAlphaOfLength(5)).thenReturn(null);
        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> actionListener = invocation.getArgument(2);
            BulkByScrollResponse response = mock(BulkByScrollResponse.class);
            actionListener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(threadPool).schedule(any(), any(), any());

        adTaskManager.cleanChildTasksAndADResultsOfDeletedTask();
        verify(client, times(2)).execute(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testDeleteADTasks() {
        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> actionListener = invocation.getArgument(2);
            BulkByScrollResponse response = mock(BulkByScrollResponse.class);
            when(response.getBulkFailures()).thenReturn(null);
            actionListener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());

        String detectorId = randomAlphaOfLength(5);
        AnomalyDetectorFunction function = mock(AnomalyDetectorFunction.class);
        ActionListener<DeleteResponse> listener = mock(ActionListener.class);
        adTaskManager.deleteADTasks(detectorId, function, listener);
        verify(function, times(1)).execute();
    }

    @SuppressWarnings("unchecked")
    public void testDeleteADTasksWithBulkFailures() {
        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> actionListener = invocation.getArgument(2);
            BulkByScrollResponse response = mock(BulkByScrollResponse.class);
            List<BulkItemResponse.Failure> failures = ImmutableList
                .of(
                    new BulkItemResponse.Failure(
                        DETECTION_STATE_INDEX,
                        randomAlphaOfLength(5),
                        new VersionConflictEngineException(new ShardId(DETECTION_STATE_INDEX, "", 1), "id", "test")
                    )
                );
            when(response.getBulkFailures()).thenReturn(failures);
            actionListener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());

        String detectorId = randomAlphaOfLength(5);
        AnomalyDetectorFunction function = mock(AnomalyDetectorFunction.class);
        ActionListener<DeleteResponse> listener = mock(ActionListener.class);
        adTaskManager.deleteADTasks(detectorId, function, listener);
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testDeleteADTasksWithException() {
        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> actionListener = invocation.getArgument(2);
            actionListener.onFailure(new IndexNotFoundException(DETECTION_STATE_INDEX));
            return null;
        }).doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> actionListener = invocation.getArgument(2);
            actionListener.onFailure(new RuntimeException("test"));
            return null;
        }).when(client).execute(any(), any(), any());

        String detectorId = randomAlphaOfLength(5);
        AnomalyDetectorFunction function = mock(AnomalyDetectorFunction.class);
        ActionListener<DeleteResponse> listener = mock(ActionListener.class);

        adTaskManager.deleteADTasks(detectorId, function, listener);
        verify(function, times(1)).execute();
        verify(listener, never()).onFailure(any());

        adTaskManager.deleteADTasks(detectorId, function, listener);
        verify(function, times(1)).execute();
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testScaleUpTaskSlots() throws IOException {
        ADTask adTask = randomAdTask(ADTaskType.HISTORICAL_HC_ENTITY);
        ActionListener<AnomalyDetectorJobResponse> listener = mock(ActionListener.class);
        when(adTaskCacheManager.getAvailableNewEntityTaskLanes(anyString())).thenReturn(0);
        doReturn(2).when(adTaskManager).detectorTaskSlotScaleDelta(anyString());
        when(adTaskCacheManager.getLastScaleEntityTaskLaneTime(anyString())).thenReturn(null);

        assertEquals(0, adTaskManager.scaleTaskSlots(adTask, transportService, listener));

        when(adTaskCacheManager.getLastScaleEntityTaskLaneTime(anyString())).thenReturn(Instant.now());
        assertEquals(2, adTaskManager.scaleTaskSlots(adTask, transportService, listener));

        when(adTaskCacheManager.getLastScaleEntityTaskLaneTime(anyString())).thenReturn(Instant.now().minus(10, ChronoUnit.DAYS));
        assertEquals(2, adTaskManager.scaleTaskSlots(adTask, transportService, listener));
        verify(adTaskCacheManager, times(1)).refreshLastScaleEntityTaskLaneTime(anyString());
        verify(adTaskManager, times(1)).forwardScaleTaskSlotRequestToLeadNode(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testForwardRequestToLeadNodeWithNotExistingNode() throws IOException {
        ADTask adTask = randomAdTask(ADTaskType.HISTORICAL_HC_ENTITY);
        ForwardADTaskRequest forwardADTaskRequest = new ForwardADTaskRequest(adTask, ADTaskAction.APPLY_FOR_TASK_SLOTS);
        ActionListener<AnomalyDetectorJobResponse> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            Consumer<Optional<DiscoveryNode>> function = invocation.getArgument(1);
            function.accept(Optional.empty());
            return null;
        }).when(hashRing).buildAndGetOwningNodeWithSameLocalAdVersion(any(), any(), any());

        adTaskManager.forwardRequestToLeadNode(forwardADTaskRequest, transportService, listener);
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testScaleTaskLaneOnCoordinatingNode() {
        ADTask adTask = mock(ADTask.class);
        when(adTask.getCoordinatingNode()).thenReturn(node1.getId());
        when(nodeFilter.getEligibleDataNodes()).thenReturn(new DiscoveryNode[] { node1, node2 });
        ActionListener<AnomalyDetectorJobResponse> listener = mock(ActionListener.class);
        adTaskManager.scaleTaskLaneOnCoordinatingNode(adTask, 2, transportService, listener);
    }

    @SuppressWarnings("unchecked")
    public void testStartDetectorWithException() throws IOException {
        AnomalyDetector detector = randomAnomalyDetector(ImmutableList.of(randomFeature(true)));
        DetectionDateRange detectionDateRange = randomDetectionDateRange();
        UserIdentity user = null;
        ActionListener<AnomalyDetectorJobResponse> listener = mock(ActionListener.class);
        when(detectionIndices.doesDetectorStateIndexExist()).thenReturn(false);
        doThrow(new RuntimeException("test")).when(detectionIndices).initDetectionStateIndex(any());
        adTaskManager.startDetector(detector, detectionDateRange, user, transportService, listener);
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testStopDetectorWithNonExistingDetector() {
        String detectorId = randomAlphaOfLength(5);
        boolean historical = true;
        ActionListener<AnomalyDetectorJobResponse> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            Consumer<Optional<AnomalyDetector>> function = invocation.getArgument(1);
            function.accept(Optional.empty());
            return null;
        }).when(adTaskManager).getDetector(anyString(), any(), any());
        adTaskManager.stopDetector(detectorId, historical, indexAnomalyDetectorJobActionHandler, null, transportService, listener);
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testStopDetectorWithNonExistingTask() {
        String detectorId = randomAlphaOfLength(5);
        boolean historical = true;
        ActionListener<AnomalyDetectorJobResponse> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            Consumer<Optional<AnomalyDetector>> function = invocation.getArgument(1);
            AnomalyDetector detector = randomAnomalyDetector(ImmutableList.of(randomFeature(true)));
            function.accept(Optional.of(detector));
            return null;
        }).when(adTaskManager).getDetector(anyString(), any(), any());

        doAnswer(invocation -> {
            ActionListener<SearchResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(null);
            return null;
        }).when(client).search(any(), any());

        adTaskManager.stopDetector(detectorId, historical, indexAnomalyDetectorJobActionHandler, null, transportService, listener);
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testStopDetectorWithTaskDone() {
        String detectorId = randomAlphaOfLength(5);
        boolean historical = true;
        ActionListener<AnomalyDetectorJobResponse> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            Consumer<Optional<AnomalyDetector>> function = invocation.getArgument(1);
            AnomalyDetector detector = randomAnomalyDetector(ImmutableList.of(randomFeature(true)));
            function.accept(Optional.of(detector));
            return null;
        }).when(adTaskManager).getDetector(anyString(), any(), any());

        doAnswer(invocation -> {
            ActionListener<SearchResponse> actionListener = invocation.getArgument(1);
            SearchHit task = SearchHit.fromXContent(TestHelpers.parser(taskContent));
            SearchHits searchHits = new SearchHits(new SearchHit[] { task }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), Float.NaN);
            InternalSearchResponse response = new InternalSearchResponse(
                searchHits,
                InternalAggregations.EMPTY,
                null,
                null,
                false,
                null,
                1
            );
            SearchResponse searchResponse = new SearchResponse(
                response,
                null,
                1,
                1,
                0,
                100,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
            actionListener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());

//        adTaskManager.stopDetector(detectorId, historical, indexAnomalyDetectorJobActionHandler, null, transportService, listener);
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testGetDetectorWithWrongContent() {
        String detectorId = randomAlphaOfLength(5);
        Consumer<Optional<AnomalyDetector>> function = mock(Consumer.class);
        ActionListener<GetResponse> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> responseListener = invocation.getArgument(1);
            GetResponse response = new GetResponse(
                new GetResult(
                    AnomalyDetector.ANOMALY_DETECTORS_INDEX,
                    detectorId,
                    UNASSIGNED_SEQ_NO,
                    0,
                    -1,
                    true,
                    BytesReference
                        .bytes(
                            new MockSimpleLog(Instant.now(), 1.0, "127.0.0.1", "category", true, "test")
                                .toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS)
                        ),
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );
            responseListener.onResponse(response);
            return null;
        }).when(client).get(any(), any());
        adTaskManager.getDetector(detectorId, function, listener);
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testDeleteTaskDocs() {
        doAnswer(invocation -> {
            ActionListener<SearchResponse> actionListener = invocation.getArgument(1);
            SearchHit task = SearchHit.fromXContent(TestHelpers.parser(taskContent));
            SearchHits searchHits = new SearchHits(new SearchHit[] { task }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), Float.NaN);
            InternalSearchResponse response = new InternalSearchResponse(
                searchHits,
                InternalAggregations.EMPTY,
                null,
                null,
                false,
                null,
                1
            );
            SearchResponse searchResponse = new SearchResponse(
                response,
                null,
                1,
                1,
                0,
                100,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
            actionListener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());

        doAnswer(invocation -> {
            ActionListener<BulkResponse> responseListener = invocation.getArgument(2);
            BulkItemResponse[] responses = new BulkItemResponse[1];
            ShardId shardId = new ShardId(new Index("index_name", "uuid"), 0);
            responses[0] = new BulkItemResponse(
                0,
                randomFrom(DocWriteRequest.OpType.values()),
                new IndexResponse(shardId, "id", 1, 1, 1, true)
            );
            responseListener.onResponse(new BulkResponse(responses, 1));
            return null;
        }).when(client).execute(any(), any(), any());

        String detectorId = randomAlphaOfLength(5);
        SearchRequest searchRequest = mock(SearchRequest.class);
        AnomalyDetectorFunction function = mock(AnomalyDetectorFunction.class);
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        adTaskManager.deleteTaskDocs(detectorId, searchRequest, function, listener);
        verify(adTaskCacheManager, times(1)).addDeletedDetectorTask(anyString());
        verify(function, times(1)).execute();
    }
}
*/
