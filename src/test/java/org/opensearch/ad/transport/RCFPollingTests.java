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

/*

public class RCFPollingTests extends AbstractADTest {
    Gson gson = new GsonBuilder().create();
    private String detectorId = "jqIG6XIBEyaF3zCMZfcB";
    private String model0Id;
    private long totalUpdates = 3L;
    private String nodeId = "abc";
    private ClusterService clusterService;
    private HashRing hashRing;
    private TransportAddress transportAddress1;
    private ModelManager manager;
    private TransportService transportService;
    private PlainActionFuture<RCFPollingResponse> future;
    private RCFPollingTransportAction action;
    private RCFPollingRequest request;
    private TransportInterceptor normalTransportInterceptor, failureTransportInterceptor;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(RCFPollingTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    private void registerHandler(FakeNode node) {
        new RCFPollingTransportAction(
            new ActionFilters(Collections.emptySet()),
            node.transportService,
            Settings.EMPTY,
            manager,
            hashRing,
            node.clusterService
        );
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        hashRing = mock(HashRing.class);
        transportAddress1 = new TransportAddress(new InetSocketAddress(InetAddress.getByName("1.2.3.4"), 9300));
        manager = mock(ModelManager.class);
        transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        future = new PlainActionFuture<>();

        request = new RCFPollingRequest(detectorId);
        model0Id = SingleStreamModelIdMapper.getRcfModelId(detectorId, 0);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            @SuppressWarnings("unchecked")
            ActionListener<Long> listener = (ActionListener<Long>) args[2];
            listener.onResponse(totalUpdates);
            return null;
        }).when(manager).getTotalUpdates(any(String.class), any(String.class), any());

        normalTransportInterceptor = new TransportInterceptor() {
            @Override
            public AsyncSender interceptSender(AsyncSender sender) {
                return new AsyncSender() {
                    @Override
                    public <T extends TransportResponse> void sendRequest(
                        Transport.Connection connection,
                        String action,
                        TransportRequest request,
                        TransportRequestOptions options,
                        TransportResponseHandler<T> handler
                    ) {
                        if (RCFPollingAction.NAME.equals(action)) {
                            sender.sendRequest(connection, action, request, options, rcfRollingHandler(handler));
                        } else {
                            sender.sendRequest(connection, action, request, options, handler);
                        }
                    }
                };
            }
        };

        failureTransportInterceptor = new TransportInterceptor() {
            @Override
            public AsyncSender interceptSender(AsyncSender sender) {
                return new AsyncSender() {
                    @Override
                    public <T extends TransportResponse> void sendRequest(
                        Transport.Connection connection,
                        String action,
                        TransportRequest request,
                        TransportRequestOptions options,
                        TransportResponseHandler<T> handler
                    ) {
                        if (RCFPollingAction.NAME.equals(action)) {
                            sender.sendRequest(connection, action, request, options, rcfFailureRollingHandler(handler));
                        } else {
                            sender.sendRequest(connection, action, request, options, handler);
                        }
                    }
                };
            }
        };
    }

    public void testDoubleNaN() {
        try {
            gson.toJson(Double.NaN);
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains("NaN is not a valid double value as per JSON specification"));
        }

        Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
        String json = gson.toJson(Double.NaN);
        assertEquals("NaN", json);
        Double value = gson.fromJson(json, Double.class);
        assertTrue(value.isNaN());
    }

    public void testNormal() {
        DiscoveryNode localNode = new DiscoveryNode(nodeId, transportAddress1, Version.CURRENT.minimumCompatibilityVersion());
        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(any(String.class))).thenReturn(Optional.of(localNode));

        when(clusterService.localNode()).thenReturn(localNode);

        action = new RCFPollingTransportAction(
            mock(ActionFilters.class),
            transportService,
            Settings.EMPTY,
            manager,
            hashRing,
            clusterService
        );
        action.doExecute(mock(Task.class), request, future);

        RCFPollingResponse response = future.actionGet();
        assertEquals(totalUpdates, response.getTotalUpdates());
    }

    public void testNoNodeFoundForModel() {
        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(any(String.class))).thenReturn(Optional.empty());
        action = new RCFPollingTransportAction(
            mock(ActionFilters.class),
            transportService,
            Settings.EMPTY,
            manager,
            hashRing,
            clusterService
        );
        action.doExecute(mock(Task.class), request, future);
        assertException(future, AnomalyDetectionException.class, RCFPollingTransportAction.NO_NODE_FOUND_MSG);
    }

    /**
     * Precondition: receiver's model manager respond with a response.  See
     *  manager.getRcfModelId mocked output in setUp method.
     * When receiving a response, respond back with totalUpdates.
     * @param handler handler for receiver
     * @return handler for request sender
     */
/*
    private <T extends TransportResponse> TransportResponseHandler<T> rcfRollingHandler(TransportResponseHandler<T> handler) {
        return new TransportResponseHandler<T>() {
            @Override
            public T read(StreamInput in) throws IOException {
                return handler.read(in);
            }

            @Override
            @SuppressWarnings("unchecked")
            public void handleResponse(T response) {
                handler.handleResponse((T) new RCFPollingResponse(totalUpdates));
            }

            @Override
            public void handleException(TransportException exp) {
                handler.handleException(exp);
            }

            @Override
            public String executor() {
                return handler.executor();
            }
        };
    }

    /**
     * Precondition: receiver's model manager respond with a response.  See
     *  manager.getRcfModelId mocked output in setUp method.
     * Create handler that would return a connection failure
     * @param handler callback handler
     * @return handlder that would return a connection failure
     */
/*
    private <T extends TransportResponse> TransportResponseHandler<T> rcfFailureRollingHandler(TransportResponseHandler<T> handler) {
        return new TransportResponseHandler<T>() {
            @Override
            public T read(StreamInput in) throws IOException {
                return handler.read(in);
            }

            @Override
            public void handleResponse(T response) {
                handler
                    .handleException(
                        new ConnectTransportException(
                            new DiscoveryNode(nodeId, transportAddress1, Version.CURRENT.minimumCompatibilityVersion()),
                            RCFPollingAction.NAME
                        )
                    );
            }

            @Override
            public void handleException(TransportException exp) {
                handler.handleException(exp);
            }

            @Override
            public String executor() {
                return handler.executor();
            }
        };
    }

    public void testGetRemoteNormalResponse() {
        setupTestNodes(normalTransportInterceptor, Settings.EMPTY);
        try {
            TransportService realTransportService = testNodes[0].transportService;
            clusterService = testNodes[0].clusterService;

            action = new RCFPollingTransportAction(
                new ActionFilters(Collections.emptySet()),
                realTransportService,
                Settings.EMPTY,
                manager,
                hashRing,
                clusterService
            );

            when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(any(String.class)))
                .thenReturn(Optional.of(testNodes[1].discoveryNode()));
            registerHandler(testNodes[1]);

            action.doExecute(null, request, future);

            RCFPollingResponse response = future.actionGet();
            assertEquals(totalUpdates, response.getTotalUpdates());
        } finally {
            tearDownTestNodes();
        }
    }

    public void testGetRemoteFailureResponse() {
        setupTestNodes(failureTransportInterceptor, Settings.EMPTY);
        try {
            TransportService realTransportService = testNodes[0].transportService;
            clusterService = testNodes[0].clusterService;

            action = new RCFPollingTransportAction(
                new ActionFilters(Collections.emptySet()),
                realTransportService,
                Settings.EMPTY,
                manager,
                hashRing,
                clusterService
            );

            when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(any(String.class)))
                .thenReturn(Optional.of(testNodes[1].discoveryNode()));
            registerHandler(testNodes[1]);

            action.doExecute(null, request, future);

            expectThrows(ConnectTransportException.class, () -> future.actionGet());
        } finally {
            tearDownTestNodes();
        }
    }

    public void testResponseToXContent() throws IOException, JsonPathNotFoundException {
        RCFPollingResponse response = new RCFPollingResponse(totalUpdates);
        String json = TestHelpers.xContentBuilderToString(response.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        assertEquals(totalUpdates, JsonDeserializer.getLongValue(json, RCFPollingResponse.TOTAL_UPDATES_KEY));
    }

    public void testRequestToXContent() throws IOException, JsonPathNotFoundException {
        RCFPollingRequest response = new RCFPollingRequest(detectorId);
        String json = TestHelpers.xContentBuilderToString(response.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        assertEquals(detectorId, JsonDeserializer.getTextValue(json, CommonName.ID_JSON_KEY));
    }

    public void testNullDetectorId() {
        String nullDetectorId = null;
        RCFPollingRequest emptyRequest = new RCFPollingRequest(nullDetectorId);
        assertTrue(emptyRequest.validate() != null);
    }
}
*/
