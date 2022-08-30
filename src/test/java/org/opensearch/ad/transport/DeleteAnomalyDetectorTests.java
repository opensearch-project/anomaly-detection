/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
package org.opensearch.ad.transport;


public class DeleteAnomalyDetectorTests extends AbstractADTest {
    private DeleteAnomalyDetectorTransportAction action;
    private TransportService transportService;
    private ActionFilters actionFilters;
    private Client client;
    private ADTaskManager adTaskManager;
    private PlainActionFuture<DeleteResponse> future;
    private DeleteResponse deleteResponse;
    private GetResponse getResponse;
    ClusterService clusterService;
    private AnomalyDetectorJob jobParameter;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(EntityProfileTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );

        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        actionFilters = mock(ActionFilters.class);
        adTaskManager = mock(ADTaskManager.class);
        action = new DeleteAnomalyDetectorTransportAction(
            transportService,
            actionFilters,
            client,
            clusterService,
            Settings.EMPTY,
            xContentRegistry(),
            adTaskManager
        );

        jobParameter = mock(AnomalyDetectorJob.class);
        when(jobParameter.getName()).thenReturn(randomAlphaOfLength(10));
        IntervalSchedule schedule = new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES);
        when(jobParameter.getSchedule()).thenReturn(schedule);
        when(jobParameter.getWindowDelay()).thenReturn(new IntervalTimeConfiguration(10, ChronoUnit.SECONDS));
    }

    public void testDeleteADTransportAction_FailDeleteResponse() {
        future = mock(PlainActionFuture.class);
        DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("1234");
        setupMocks(true, true, false, false);

        action.doExecute(mock(Task.class), request, future);
        verify(adTaskManager).deleteADTasks(eq("1234"), any(), any());
        verify(client, times(1)).delete(any(), any());
        verify(future).onFailure(any(OpenSearchStatusException.class));
    }

    public void testDeleteADTransportAction_NullAnomalyDetector() {
        future = mock(PlainActionFuture.class);
        DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("1234");
        setupMocks(true, false, false, false);

        action.doExecute(mock(Task.class), request, future);
        verify(adTaskManager).deleteADTasks(eq("1234"), any(), any());
        verify(client, times(3)).delete(any(), any());
    }

    public void testDeleteADTransportAction_DeleteResponseException() {
        future = mock(PlainActionFuture.class);
        DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("1234");
        setupMocks(true, false, true, false);

        action.doExecute(mock(Task.class), request, future);
        verify(adTaskManager).deleteADTasks(eq("1234"), any(), any());
        verify(client, times(1)).delete(any(), any());
        verify(future).onFailure(any(RuntimeException.class));
    }

    public void testDeleteADTransportAction_LatestDetectorLevelTask() {
        when(clusterService.state()).thenReturn(createClusterState());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Consumer<Optional<ADTask>> consumer = (Consumer<Optional<ADTask>>) args[2];
            ADTask adTask = ADTask.builder().state("RUNNING").build();
            consumer.accept(Optional.of(adTask));
            return null;
        }).when(adTaskManager).getAndExecuteOnLatestDetectorLevelTask(eq("1234"), any(), any(), eq(transportService), eq(true), any());

        future = mock(PlainActionFuture.class);
        DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("1234");
        setupMocks(false, false, false, false);

        action.doExecute(mock(Task.class), request, future);
        verify(future).onFailure(any(OpenSearchStatusException.class));
    }

    public void testDeleteADTransportAction_JobRunning() {
        when(clusterService.state()).thenReturn(createClusterState());
        future = mock(PlainActionFuture.class);
        DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("1234");
        setupMocks(false, false, false, false);

        action.doExecute(mock(Task.class), request, future);
        verify(future).onFailure(any(RuntimeException.class));
    }

    public void testDeleteADTransportAction_GetResponseException() {
        when(clusterService.state()).thenReturn(createClusterState());
        future = mock(PlainActionFuture.class);
        DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("1234");
        setupMocks(false, false, false, true);

        action.doExecute(mock(Task.class), request, future);
        verify(client).get(any(), any());
        verify(client).get(any(), any());
    }

    private ClusterState createClusterState() {
        ImmutableOpenMap<String, IndexMetadata> immutableOpenMap = ImmutableOpenMap
            .<String, IndexMetadata>builder()
            .fPut(
                ANOMALY_DETECTOR_JOB_INDEX,
                IndexMetadata
                    .builder("test")
                    .settings(
                        Settings
                            .builder()
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 1)
                            .put("index.version.created", Version.CURRENT.id)
                    )
                    .build()
            )
            .build();
        Metadata metaData = Metadata.builder().indices(immutableOpenMap).build();
        ClusterState clusterState = new ClusterState(new ClusterName("test_name"), 1l, "uuid", metaData, null, null, null, null, 1, true);
        return clusterState;
    }

    private void setupMocks(
        boolean nullAnomalyDetectorResponse,
        boolean failDeleteDeleteResponse,
        boolean deleteResponseException,
        boolean getResponseFailure
    ) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Consumer<Optional<AnomalyDetector>> consumer = (Consumer<Optional<AnomalyDetector>>) args[1];
            if (nullAnomalyDetectorResponse) {
                consumer.accept(Optional.empty());
            } else {
                AnomalyDetector ad = mock(AnomalyDetector.class);
                consumer.accept(Optional.of(ad));
            }
            return null;
        }).when(adTaskManager).getDetector(any(), any(), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            AnomalyDetectorFunction function = (AnomalyDetectorFunction) args[1];

            function.execute();
            return null;
        }).when(adTaskManager).deleteADTasks(eq("1234"), any(), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<DeleteResponse> listener = (ActionListener<DeleteResponse>) args[1];
            deleteResponse = mock(DeleteResponse.class);
            if (deleteResponseException) {
                listener.onFailure(new RuntimeException("Failed to delete anomaly detector job"));
                return null;
            }
            if (failDeleteDeleteResponse) {
                doReturn(DocWriteResponse.Result.CREATED).when(deleteResponse).getResult();
            } else {
                doReturn(DocWriteResponse.Result.DELETED).when(deleteResponse).getResult();
            }
            listener.onResponse(deleteResponse);
            return null;
        }).when(client).delete(any(), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];
            if (getResponseFailure) {
                listener.onFailure(new RuntimeException("Fail to get anomaly detector job"));
                return null;
            }
            getResponse = new GetResponse(
                new GetResult(
                    AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX,
                    "id",
                    UNASSIGNED_SEQ_NO,
                    0,
                    -1,
                    true,
                    BytesReference
                        .bytes(
                            new AnomalyDetectorJob(
                                "1234",
                                jobParameter.getSchedule(),
                                jobParameter.getWindowDelay(),
                                true,
                                Instant.now().minusSeconds(60),
                                Instant.now(),
                                Instant.now(),
                                60L,
                                TestHelpers.randomUser(),
                                jobParameter.getResultIndex()
                            ).toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS)
                        ),
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );
            listener.onResponse(getResponse);
            return null;
        }).when(client).get(any(), any());
    }
}
*/
