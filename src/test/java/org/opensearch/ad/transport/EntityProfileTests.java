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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.transport;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.caching.EntityCache;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.JsonPathNotFoundException;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.model.EntityProfileName;
import org.opensearch.ad.model.ModelProfile;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.tasks.Task;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import test.org.opensearch.ad.util.FakeNode;
import test.org.opensearch.ad.util.JsonDeserializer;

public class EntityProfileTests extends AbstractADTest {
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
    private ModelManager modelManager;
    private ClusterService clusterService;
    private CacheProvider cacheProvider;
    private EntityProfileTransportAction action;
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
            Collections.emptySet()
        );
        settings = Settings.EMPTY;

        modelManager = mock(ModelManager.class);
        modelId = "yecrdnUBqurvo9uKU_d8_entity_app_0";
        when(modelManager.getEntityModelId(anyString(), anyString())).thenReturn(modelId);

        clusterService = mock(ClusterService.class);

        cacheProvider = mock(CacheProvider.class);
        EntityCache cache = mock(EntityCache.class);
        updates = 1L;
        when(cache.getTotalUpdates(anyString(), anyString())).thenReturn(updates);
        when(cache.isActive(anyString(), anyString())).thenReturn(isActive);
        when(cache.getLastActiveMs(anyString(), anyString())).thenReturn(lastActiveTimestamp);
        when(cache.getModelSize(anyString(), anyString())).thenReturn(modelSize);
        when(cacheProvider.get()).thenReturn(cache);

        action = new EntityProfileTransportAction(
            actionFilters,
            transportService,
            settings,
            modelManager,
            hashRing,
            clusterService,
            cacheProvider
        );

        future = new PlainActionFuture<>();
        transportAddress1 = new TransportAddress(new InetSocketAddress(InetAddress.getByName("1.2.3.4"), 9300));

        request = new EntityProfileRequest(detectorId, entityValue, state);

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
                        if (EntityProfileAction.NAME.equals(action)) {
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
                        if (EntityProfileAction.NAME.equals(action)) {
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
                            EntityProfileAction.NAME
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
        new EntityProfileTransportAction(
            new ActionFilters(Collections.emptySet()),
            node.transportService,
            Settings.EMPTY,
            modelManager,
            hashRing,
            node.clusterService,
            cacheProvider
        );
    }

    public void testInvalidRequest() {
        when(hashRing.getOwningNode(anyString())).thenReturn(Optional.empty());
        action.doExecute(task, request, future);

        assertException(future, AnomalyDetectionException.class, EntityProfileTransportAction.NO_NODE_FOUND_MSG);
    }

    public void testLocalNodeHit() {
        DiscoveryNode localNode = new DiscoveryNode(nodeId, transportAddress1, Version.CURRENT.minimumCompatibilityVersion());
        when(hashRing.getOwningNode(anyString())).thenReturn(Optional.of(localNode));
        when(clusterService.localNode()).thenReturn(localNode);

        action.doExecute(task, request, future);
        EntityProfileResponse response = future.actionGet(20_000);
        assertEquals(updates, response.getTotalUpdates());
    }

    public void testAllHit() {
        DiscoveryNode localNode = new DiscoveryNode(nodeId, transportAddress1, Version.CURRENT.minimumCompatibilityVersion());
        when(hashRing.getOwningNode(anyString())).thenReturn(Optional.of(localNode));
        when(clusterService.localNode()).thenReturn(localNode);

        request = new EntityProfileRequest(detectorId, entityValue, all);
        action.doExecute(task, request, future);

        EntityProfileResponse expectedResponse = new EntityProfileResponse(
            isActive,
            lastActiveTimestamp,
            updates,
            new ModelProfile(modelId, modelSize, nodeId)
        );
        EntityProfileResponse response = future.actionGet(20_000);
        assertEquals(expectedResponse, response);
    }

    public void testGetRemoteUpdateResponse() {
        setupTestNodes(Settings.EMPTY, normalTransportInterceptor);
        try {
            TransportService realTransportService = testNodes[0].transportService;
            clusterService = testNodes[0].clusterService;

            action = new EntityProfileTransportAction(
                actionFilters,
                realTransportService,
                settings,
                modelManager,
                hashRing,
                clusterService,
                cacheProvider
            );

            when(hashRing.getOwningNode(any(String.class))).thenReturn(Optional.of(testNodes[1].discoveryNode()));
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
        setupTestNodes(Settings.EMPTY, failureTransportInterceptor);
        try {
            TransportService realTransportService = testNodes[0].transportService;
            clusterService = testNodes[0].clusterService;

            action = new EntityProfileTransportAction(
                actionFilters,
                realTransportService,
                settings,
                modelManager,
                hashRing,
                clusterService,
                cacheProvider
            );

            when(hashRing.getOwningNode(any(String.class))).thenReturn(Optional.of(testNodes[1].discoveryNode()));
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
        builder.setModelProfile(new ModelProfile(modelId, modelSize, nodeId));
        EntityProfileResponse response = builder.build();
        String json = TestHelpers.xContentBuilderToString(response.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        assertEquals(lastActiveTimestamp, JsonDeserializer.getLongValue(json, EntityProfileResponse.LAST_ACTIVE_TS));
        assertEquals(modelSize, JsonDeserializer.getChildNode(json, CommonName.MODEL, ModelProfile.MODEL_SIZE_IN_BYTES).getAsLong());
    }

    public void testSerialzationResponse() throws IOException {
        EntityProfileResponse.Builder builder = new EntityProfileResponse.Builder();
        builder.setLastActiveMs(lastActiveTimestamp).build();
        ModelProfile model = new ModelProfile(modelId, modelSize, nodeId);
        builder.setModelProfile(model);
        EntityProfileResponse response = builder.build();

        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        EntityProfileResponse readResponse = EntityProfileAction.INSTANCE.getResponseReader().read(streamInput);
        assertThat(response.getModelProfile(), equalTo(readResponse.getModelProfile()));
        assertThat(response.getLastActiveMs(), equalTo(readResponse.getLastActiveMs()));
    }

    public void testResponseHashCodeEquals() {
        EntityProfileResponse.Builder builder = new EntityProfileResponse.Builder();
        builder.setLastActiveMs(lastActiveTimestamp).build();
        ModelProfile model = new ModelProfile(modelId, modelSize, nodeId);
        builder.setModelProfile(model);
        EntityProfileResponse response = builder.build();

        HashSet<EntityProfileResponse> set = new HashSet<>();
        assertTrue(false == set.contains(response));
        set.add(response);
        assertTrue(set.contains(response));
    }
}
