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

package org.opensearch.timeseries.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.common.exception.JsonPathNotFoundException;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.transport.ADEntityProfileAction;
import org.opensearch.ad.transport.ADEntityProfileTransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
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
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.EntityProfileName;
import org.opensearch.timeseries.model.ModelProfile;
import org.opensearch.timeseries.model.ModelProfileOnNode;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import test.org.opensearch.ad.util.FakeNode;
import test.org.opensearch.ad.util.JsonDeserializer;

public class EntityProfileTests extends AbstractTimeSeriesTest {
    private String detectorId = "yecrdnUBqurvo9uKU_d8";
    private String entityValue = "app_0";
    private String nodeId = "abc";
    private Set<EntityProfileName> state;
    private Set<EntityProfileName> all;
    private Set<EntityProfileName> model;
    private HashRing hashRing;
    private ActionFilters actionFilters;
    private TransportService transportService;
    private Settings settings;
    private ClusterService clusterService;
    private ADCacheProvider cacheProvider;
    private ADEntityProfileTransportAction action;
    private Task task;
    private PlainActionFuture<EntityProfileResponse> future;
    private TransportAddress transportAddress1;
    private long updates;
    private EntityProfileRequest request;
    private String modelId;
    private long lastActiveTimestamp = 1603989830158L;
    private long modelSize = 712480L;
    private boolean isActive = Boolean.TRUE;
    private TransportInterceptor normalTransportInterceptor, failureTransportInterceptor;
    private String categoryName = "field";
    private Entity entity;

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
        state = new HashSet<EntityProfileName>();
        state.add(EntityProfileName.STATE);

        all = new HashSet<EntityProfileName>();
        all.add(EntityProfileName.INIT_PROGRESS);
        all.add(EntityProfileName.ENTITY_INFO);
        all.add(EntityProfileName.MODELS);

        model = new HashSet<EntityProfileName>();
        model.add(EntityProfileName.MODELS);

        hashRing = mock(HashRing.class);
        actionFilters = mock(ActionFilters.class);
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
        settings = Settings.EMPTY;

        modelId = "yecrdnUBqurvo9uKU_d8_entity_app_0";

        clusterService = mock(ClusterService.class);

        cacheProvider = mock(ADCacheProvider.class);
        ADPriorityCache cache = mock(ADPriorityCache.class);
        updates = 1L;
        when(cache.getTotalUpdates(anyString(), anyString())).thenReturn(updates);
        when(cache.isActive(anyString(), anyString())).thenReturn(isActive);
        when(cache.getLastActiveTime(anyString(), anyString())).thenReturn(lastActiveTimestamp);
        Map<String, Long> modelSizeMap = new HashMap<>();
        modelSizeMap.put(modelId, modelSize);
        when(cache.getModelSize(anyString())).thenReturn(modelSizeMap);
        when(cacheProvider.get()).thenReturn(cache);

        action = new ADEntityProfileTransportAction(actionFilters, transportService, settings, hashRing, clusterService, cacheProvider);

        future = new PlainActionFuture<>();
        transportAddress1 = new TransportAddress(new InetSocketAddress(InetAddress.getByName("1.2.3.4"), 9300));

        entity = Entity.createSingleAttributeEntity(categoryName, entityValue);

        request = new EntityProfileRequest(detectorId, entity, state);

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
                        if (ADEntityProfileAction.NAME.equals(action)) {
                            sender.sendRequest(connection, action, request, options, entityProfileHandler(handler));
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
                        if (ADEntityProfileAction.NAME.equals(action)) {
                            sender.sendRequest(connection, action, request, options, entityFailureProfileandler(handler));
                        } else {
                            sender.sendRequest(connection, action, request, options, handler);
                        }
                    }
                };
            }
        };
    }

    private <T extends TransportResponse> TransportResponseHandler<T> entityProfileHandler(TransportResponseHandler<T> handler) {
        return new TransportResponseHandler<T>() {
            @Override
            public T read(StreamInput in) throws IOException {
                return handler.read(in);
            }

            @Override
            @SuppressWarnings("unchecked")
            public void handleResponse(T response) {
                handler.handleResponse((T) new EntityProfileResponse.Builder().setTotalUpdates(updates).build());
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

    private <T extends TransportResponse> TransportResponseHandler<T> entityFailureProfileandler(TransportResponseHandler<T> handler) {
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
                            ADEntityProfileAction.NAME
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

    private void registerHandler(FakeNode node) {
        new ADEntityProfileTransportAction(
            new ActionFilters(Collections.emptySet()),
            node.transportService,
            Settings.EMPTY,
            hashRing,
            node.clusterService,
            cacheProvider
        );
    }

    public void testInvalidRequest() {
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(anyString())).thenReturn(Optional.empty());
        action.doExecute(task, request, future);

        assertException(future, TimeSeriesException.class, ADEntityProfileTransportAction.NO_NODE_FOUND_MSG);
    }

    public void testLocalNodeHit() {
        DiscoveryNode localNode = new DiscoveryNode(nodeId, transportAddress1, Version.CURRENT.minimumCompatibilityVersion());
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(anyString())).thenReturn(Optional.of(localNode));
        when(clusterService.localNode()).thenReturn(localNode);

        action.doExecute(task, request, future);
        EntityProfileResponse response = future.actionGet(20_000);
        assertEquals(updates, response.getTotalUpdates());
    }

    public void testAllHit() {
        DiscoveryNode localNode = new DiscoveryNode(nodeId, transportAddress1, Version.CURRENT.minimumCompatibilityVersion());
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(anyString())).thenReturn(Optional.of(localNode));
        when(clusterService.localNode()).thenReturn(localNode);

        request = new EntityProfileRequest(detectorId, entity, all);
        action.doExecute(task, request, future);

        EntityProfileResponse expectedResponse = new EntityProfileResponse(isActive, lastActiveTimestamp, updates, null);
        EntityProfileResponse response = future.actionGet(20_000);
        assertEquals(expectedResponse, response);
    }

    public void testGetRemoteUpdateResponse() {
        setupTestNodes(normalTransportInterceptor);
        try {
            TransportService realTransportService = testNodes[0].transportService;
            clusterService = testNodes[0].clusterService;

            action = new ADEntityProfileTransportAction(
                actionFilters,
                realTransportService,
                settings,
                hashRing,
                clusterService,
                cacheProvider
            );

            when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(any(String.class)))
                .thenReturn(Optional.of(testNodes[1].discoveryNode()));
            registerHandler(testNodes[1]);

            action.doExecute(null, request, future);

            EntityProfileResponse expectedResponse = new EntityProfileResponse(null, -1L, updates, null);

            EntityProfileResponse response = future.actionGet(10_000);
            assertEquals(expectedResponse, response);
        } finally {
            tearDownTestNodes();
        }
    }

    public void testGetRemoteFailureResponse() {
        setupTestNodes(failureTransportInterceptor);
        try {
            TransportService realTransportService = testNodes[0].transportService;
            clusterService = testNodes[0].clusterService;

            action = new ADEntityProfileTransportAction(
                actionFilters,
                realTransportService,
                settings,
                hashRing,
                clusterService,
                cacheProvider
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
        long lastActiveTimestamp = 10L;
        EntityProfileResponse.Builder builder = new EntityProfileResponse.Builder();
        builder.setLastActiveMs(lastActiveTimestamp).build();
        builder.setModelProfile(new ModelProfileOnNode(nodeId, new ModelProfile(modelId, entity, modelSize)));
        EntityProfileResponse response = builder.build();
        String json = TestHelpers.xContentBuilderToString(response.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        assertEquals(lastActiveTimestamp, JsonDeserializer.getLongValue(json, EntityProfileResponse.LAST_ACTIVE_TS));
        assertEquals(modelSize, JsonDeserializer.getChildNode(json, CommonName.MODEL, CommonName.MODEL_SIZE_IN_BYTES).getAsLong());
    }

    public void testResponseHashCodeEquals() {
        EntityProfileResponse.Builder builder = new EntityProfileResponse.Builder();
        builder.setLastActiveMs(lastActiveTimestamp).build();
        ModelProfileOnNode model = new ModelProfileOnNode(nodeId, new ModelProfile(modelId, entity, modelSize));
        builder.setModelProfile(model);
        EntityProfileResponse response = builder.build();

        HashSet<EntityProfileResponse> set = new HashSet<>();
        assertTrue(false == set.contains(response));
        set.add(response);
        assertTrue(set.contains(response));
    }

    public void testEntityProfileName() {
        assertEquals("state", EntityProfileName.getName(CommonName.STATE).getName());
        assertEquals("models", EntityProfileName.getName(CommonName.MODELS).getName());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> EntityProfileName.getName("abc"));
        assertEquals(exception.getMessage(), ADCommonMessages.UNSUPPORTED_PROFILE_TYPE);
    }
}
