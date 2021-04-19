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

package com.amazon.opendistroforelasticsearch.ad.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import test.com.amazon.opendistroforelasticsearch.ad.util.MLUtil;

import com.amazon.opendistroforelasticsearch.ad.AbstractADTest;
import com.amazon.opendistroforelasticsearch.ad.NodeStateManager;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.caching.CacheProvider;
import com.amazon.opendistroforelasticsearch.ad.caching.EntityCache;
import com.amazon.opendistroforelasticsearch.ad.cluster.HashRing;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.InternalFailure;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.feature.SearchFeatureDao;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelPartitioner;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingResult;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStat;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.stats.StatNames;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.CounterSupplier;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.MultiEntityResultHandler;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;

public class MultientityResultTests extends AbstractADTest {
    private AnomalyResultTransportAction action;
    private AnomalyResultRequest request;
    private TransportInterceptor entityResultInterceptor;
    private Clock clock;
    private AnomalyDetector detector;
    private NodeStateManager stateManager;
    private static Settings settings;
    private TransportService transportService;
    private SearchFeatureDao searchFeatureDao;
    private Client client;
    private FeatureManager featureQuery;
    private ModelManager normalModelManager;
    private ModelPartitioner normalModelPartitioner;
    private HashRing hashRing;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameResolver;
    private ADCircuitBreakerService adCircuitBreakerService;
    private ADStats adStats;
    private ThreadPool mockThreadPool;
    private String detectorId;
    private Instant now;
    private String modelId;
    private MultiEntityResultHandler anomalyResultHandler;
    private CheckpointDao checkpointDao;
    private CacheProvider provider;
    private AnomalyDetectionIndices indexUtil;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyResultTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @SuppressWarnings({ "serial", "unchecked" })
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        now = Instant.now();
        clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        detectorId = "123";
        modelId = "abc";
        String categoryField = "a";
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Collections.singletonList(categoryField));

        stateManager = mock(NodeStateManager.class);
        // make sure parameters are not null, otherwise this mock won't get invoked
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(stateManager).getAnomalyDetector(anyString(), any(ActionListener.class));
        when(stateManager.getLastIndexThrottledTime()).thenReturn(Instant.MIN);

        settings = Settings.builder().put(AnomalyDetectorSettings.COOLDOWN_MINUTES.getKey(), TimeValue.timeValueMinutes(5)).build();

        request = new AnomalyResultRequest(detectorId, 100, 200);

        transportService = mock(TransportService.class);

        client = mock(Client.class);
        ThreadContext threadContext = new ThreadContext(settings);
        mockThreadPool = mock(ThreadPool.class);
        setUpADThreadPool(mockThreadPool);
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);

        featureQuery = mock(FeatureManager.class);

        normalModelManager = mock(ModelManager.class);
        when(normalModelManager.getEntityModelId(anyString(), anyString())).thenReturn(modelId);

        normalModelPartitioner = mock(ModelPartitioner.class);

        hashRing = mock(HashRing.class);

        clusterService = mock(ClusterService.class);

        indexNameResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));

        adCircuitBreakerService = mock(ADCircuitBreakerService.class);
        when(adCircuitBreakerService.isOpen()).thenReturn(false);

        IndexUtils indexUtils = new IndexUtils(client, mock(ClientUtil.class), clusterService, indexNameResolver);
        Map<String, ADStat<?>> statsMap = new HashMap<String, ADStat<?>>() {
            {
                put(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
                put(StatNames.AD_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
                put(StatNames.AD_HC_EXECUTE_REQUEST_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
                put(StatNames.AD_HC_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
            }
        };
        adStats = new ADStats(indexUtils, normalModelManager, statsMap);

        searchFeatureDao = mock(SearchFeatureDao.class);

        action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            stateManager,
            featureQuery,
            normalModelManager,
            normalModelPartitioner,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool,
            searchFeatureDao
        );

        anomalyResultHandler = mock(MultiEntityResultHandler.class);
        checkpointDao = mock(CheckpointDao.class);
        provider = mock(CacheProvider.class);
        indexUtil = mock(AnomalyDetectionIndices.class);
    }

    @Override
    @After
    public final void tearDown() throws Exception {
        tearDownTestNodes();
        super.tearDown();
    }

    @SuppressWarnings("unchecked")
    public void testQueryError() {
        // non-EndRunException won't stop action from running
        when(stateManager.fetchColdStartException(anyString())).thenReturn(Optional.of(new AnomalyDetectionException(detectorId, "")));

        doAnswer(invocation -> {
            ActionListener<Map<String, double[]>> listener = invocation.getArgument(3);
            listener
                .onFailure(
                    new EndRunException(
                        detectorId,
                        CommonErrorMessages.INVALID_SEARCH_QUERY_MSG,
                        new NoSuchElementException("No value present"),
                        false
                    )
                );
            return null;
        }).when(searchFeatureDao).getFeaturesByEntities(any(), anyLong(), anyLong(), any());

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        verify(stateManager, times(1)).getAnomalyDetector(anyString(), any(ActionListener.class));

        assertException(listener, EndRunException.class, CommonErrorMessages.INVALID_SEARCH_QUERY_MSG);
    }

    public void testIndexNotFound() {
        // non-EndRunException won't stop action from running
        when(stateManager.fetchColdStartException(anyString())).thenReturn(Optional.of(new AnomalyDetectionException(detectorId, "")));

        doAnswer(invocation -> {
            ActionListener<Map<String, double[]>> listener = invocation.getArgument(3);
            listener.onFailure(new IndexNotFoundException("", ""));
            return null;
        }).when(searchFeatureDao).getFeaturesByEntities(any(), anyLong(), anyLong(), any());

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        assertException(listener, EndRunException.class, AnomalyResultTransportAction.TROUBLE_QUERYING_ERR_MSG);
    }

    public void testColdStartEndRunException() {
        when(stateManager.fetchColdStartException(anyString()))
            .thenReturn(
                Optional
                    .of(
                        new EndRunException(
                            detectorId,
                            CommonErrorMessages.INVALID_SEARCH_QUERY_MSG,
                            new NoSuchElementException("No value present"),
                            false
                        )
                    )
            );
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);
        assertException(listener, EndRunException.class, CommonErrorMessages.INVALID_SEARCH_QUERY_MSG);
    }

    public void testEmptyFeatures() {
        doAnswer(invocation -> {
            ActionListener<Map<String, double[]>> listener = invocation.getArgument(3);
            listener.onResponse(new HashMap<String, double[]>());
            return null;
        }).when(searchFeatureDao).getFeaturesByEntities(any(), anyLong(), anyLong(), any());

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);
    }

    private <T extends TransportResponse> TransportResponseHandler<T> entityResultHandler(TransportResponseHandler<T> handler) {
        return new TransportResponseHandler<T>() {
            @Override
            public T read(StreamInput in) throws IOException {
                return handler.read(in);
            }

            @Override
            @SuppressWarnings("unchecked")
            public void handleResponse(T response) {
                handler.handleResponse(response);
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

    private <T extends TransportResponse> TransportResponseHandler<T> unackEntityResultHandler(TransportResponseHandler<T> handler) {
        return new TransportResponseHandler<T>() {
            @Override
            public T read(StreamInput in) throws IOException {
                return handler.read(in);
            }

            @Override
            @SuppressWarnings("unchecked")
            public void handleResponse(T response) {
                handler.handleResponse((T) new AcknowledgedResponse(false));
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

    private void setUpEntityResult() {
        // register entity result action
        new EntityResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            // since we send requests to testNodes[1]
            testNodes[1].transportService,
            normalModelManager,
            adCircuitBreakerService,
            anomalyResultHandler,
            checkpointDao,
            provider,
            stateManager,
            settings,
            clock,
            indexUtil
        );

        EntityCache entityCache = mock(EntityCache.class);
        when(provider.get()).thenReturn(entityCache);
        when(entityCache.get(any(), any(), any(), anyString())).thenReturn(MLUtil.randomNonEmptyModelState());

        when(normalModelManager.getAnomalyResultForEntity(anyString(), any(), anyString(), any(), anyString()))
            .thenReturn(new ThresholdingResult(0, 1, 1));
    }

    private <T extends TransportResponse> void setUpTransportInterceptor(
        Function<TransportResponseHandler<T>, TransportResponseHandler<T>> interceptor
    ) {
        doAnswer(invocation -> {
            ActionListener<Map<String, double[]>> listener = invocation.getArgument(3);
            Map<String, double[]> features = new HashMap<String, double[]>();
            features.put("1.0.2.3", new double[] { 0 });
            features.put("2.0.2.3", new double[] { 1 });
            listener.onResponse(features);
            return null;
        }).when(searchFeatureDao).getFeaturesByEntities(any(), anyLong(), anyLong(), any());

        entityResultInterceptor = new TransportInterceptor() {
            @Override
            public AsyncSender interceptSender(AsyncSender sender) {
                return new AsyncSender() {
                    @SuppressWarnings("unchecked")
                    @Override
                    public <T2 extends TransportResponse> void sendRequest(
                        Transport.Connection connection,
                        String action,
                        TransportRequest request,
                        TransportRequestOptions options,
                        TransportResponseHandler<T2> handler
                    ) {
                        if (action.equals(EntityResultAction.NAME)) {
                            sender
                                .sendRequest(
                                    connection,
                                    action,
                                    request,
                                    options,
                                    interceptor.apply((TransportResponseHandler<T>) handler)
                                );
                        } else {
                            sender.sendRequest(connection, action, request, options, handler);
                        }
                    }
                };
            }
        };

        setupTestNodes(settings, entityResultInterceptor);

        // mock hashing ring response. This has to happen after setting up test nodes with the failure interceptor
        when(hashRing.getOwningNode(any(String.class))).thenReturn(Optional.of(testNodes[1].discoveryNode()));

        TransportService realTransportService = testNodes[0].transportService;
        ClusterService realClusterService = testNodes[0].clusterService;

        action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            realTransportService,
            settings,
            client,
            stateManager,
            featureQuery,
            normalModelManager,
            normalModelPartitioner,
            hashRing,
            realClusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            searchFeatureDao
        );
    }

    public void testNonEmptyFeatures() {
        setUpTransportInterceptor(this::entityResultHandler);
        setUpEntityResult();

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(0d, response.getAnomalyGrade(), 0.01);
    }

    public void testCircuitBreakerOpen() {
        setUpTransportInterceptor(this::entityResultHandler);

        ADCircuitBreakerService openBreaker = mock(ADCircuitBreakerService.class);
        when(openBreaker.isOpen()).thenReturn(true);
        // register entity result action
        new EntityResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            // since we send requests to testNodes[1]
            testNodes[1].transportService,
            normalModelManager,
            openBreaker,
            anomalyResultHandler,
            checkpointDao,
            provider,
            stateManager,
            settings,
            clock,
            indexUtil
        );

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);
        assertException(listener, LimitExceededException.class, CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG);
    }

    public void testNotAck() {
        setUpTransportInterceptor(this::unackEntityResultHandler);
        setUpEntityResult();

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        assertException(listener, InternalFailure.class, AnomalyResultTransportAction.NO_ACK_ERR);
        verify(stateManager, times(1)).addPressure(anyString());
    }
}
