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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Optional;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.common.exception.JsonPathNotFoundException;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.ml.SingleStreamModelIdMapper;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import test.org.opensearch.ad.util.FakeNode;
import test.org.opensearch.ad.util.JsonDeserializer;

public class RCFPollingTests extends AbstractTimeSeriesTest {
    Gson gson = new GsonBuilder().create();
    private String detectorId = "jqIG6XIBEyaF3zCMZfcB";
    private String model0Id;
    private long totalUpdates = 3L;
    private String nodeId = "abc";
    private ClusterService clusterService;
    private HashRing hashRing;
    private TransportAddress transportAddress1;
    private ADModelManager manager;
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
        manager = mock(ADModelManager.class);
        transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
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
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(any(String.class))).thenReturn(Optional.of(localNode));

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
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(any(String.class))).thenReturn(Optional.empty());
        action = new RCFPollingTransportAction(
                mock(ActionFilters.class),
                transportService,
                Settings.EMPTY,
                manager,
                hashRing,
                clusterService
                );
        action.doExecute(mock(Task.class), request, future);
        assertException(future, TimeSeriesException.class, RCFPollingTransportAction.NO_NODE_FOUND_MSG);
    }

    /**
     * Precondition: receiver's model manager respond with a response.  See
     *  manager.getRcfModelId mocked output in setUp method.
     * When receiving a response, respond back with totalUpdates.
     * @param handler handler for receiver
     * @return handler for request sender
     */
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

            when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(any(String.class)))
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

            when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(any(String.class)))
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
        assertEquals(detectorId, JsonDeserializer.getTextValue(json, ADCommonName.ID_JSON_KEY));
    }

    public void testNullDetectorId() {
        String nullDetectorId = null;
        RCFPollingRequest emptyRequest = new RCFPollingRequest(nullDetectorId);
        assertTrue(emptyRequest.validate() != null);
    }
}
