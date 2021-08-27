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

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.TestHelpers.createIndexBlockedState;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.common.exception.InternalFailure;
import org.opensearch.ad.common.exception.JsonPathNotFoundException;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.feature.SinglePointFeatures;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ModelPartitioner;
import org.opensearch.ad.ml.RcfResult;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.ml.rcf.CombinedRcfResult;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorInternalState;
import org.opensearch.ad.model.FeatureData;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStat;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.stats.StatNames;
import org.opensearch.ad.stats.suppliers.CounterSupplier;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NotSerializableExceptionWrapper;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.shard.ShardId;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.NodeNotConnectedException;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import test.org.opensearch.ad.util.JsonDeserializer;

import com.google.gson.JsonElement;

public class AnomalyResultTests extends AbstractADTest {
    private Settings settings;
    private TransportService transportService;
    private ClusterService clusterService;
    private NodeStateManager stateManager;
    private FeatureManager featureQuery;
    private ModelManager normalModelManager;
    private ModelPartitioner normalModelPartitioner;
    private Client client;
    private AnomalyDetector detector;
    private HashRing hashRing;
    private IndexNameExpressionResolver indexNameResolver;
    private String rcfModelIDPrefix;
    private String thresholdModelID;
    private String adID;
    private String featureId;
    private String featureName;
    private ADCircuitBreakerService adCircuitBreakerService;
    private ADStats adStats;
    private int partitionNum;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyResultTests.class.getSimpleName());
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
        super.setUpLog4jForJUnit(AnomalyResultTransportAction.class);

        setupTestNodes(AnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY, AnomalyDetectorSettings.PAGE_SIZE);

        transportService = testNodes[0].transportService;
        clusterService = testNodes[0].clusterService;
        settings = clusterService.getSettings();

        stateManager = mock(NodeStateManager.class);
        // return 2 RCF partitions
        partitionNum = 2;
        when(stateManager.getPartitionNumber(any(String.class), any(AnomalyDetector.class))).thenReturn(partitionNum);
        when(stateManager.isMuted(any(String.class), any(String.class))).thenReturn(false);
        when(stateManager.markColdStartRunning(anyString())).thenReturn(() -> {});

        detector = mock(AnomalyDetector.class);
        featureId = "xyz";
        // we have one feature
        when(detector.getEnabledFeatureIds()).thenReturn(Collections.singletonList(featureId));
        featureName = "abc";
        when(detector.getEnabledFeatureNames()).thenReturn(Collections.singletonList(featureName));
        List<String> userIndex = new ArrayList<>();
        userIndex.add("test*");
        when(detector.getIndices()).thenReturn(userIndex);
        adID = "123";
        when(detector.getDetectorId()).thenReturn(adID);
        when(detector.getCategoryField()).thenReturn(null);
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(stateManager).getAnomalyDetector(any(String.class), any(ActionListener.class));

        hashRing = mock(HashRing.class);
        Optional<DiscoveryNode> localNode = Optional.of(clusterService.state().nodes().getLocalNode());
        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeJob(any(String.class))).thenReturn(localNode);
        doReturn(localNode).when(hashRing).getNodeByAddress(any());
        featureQuery = mock(FeatureManager.class);

        doAnswer(invocation -> {
            ActionListener<SinglePointFeatures> listener = invocation.getArgument(3);
            listener.onResponse(new SinglePointFeatures(Optional.of(new double[] { 0.0d }), Optional.of(new double[] { 0 })));
            return null;
        }).when(featureQuery).getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), any(ActionListener.class));

        double rcfScore = 0.2;
        normalModelManager = mock(ModelManager.class);
        doAnswer(invocation -> {
            ActionListener<RcfResult> listener = invocation.getArgument(3);
            listener.onResponse(new RcfResult(0.2, 0, 100, new double[] { 1 }));
            return null;
        }).when(normalModelManager).getRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));
        when(normalModelManager.combineRcfResults(any(), anyInt())).thenReturn(new CombinedRcfResult(0, 1.0d, new double[] { 1 }));

        doAnswer(invocation -> {
            ActionListener<ThresholdingResult> listener = invocation.getArgument(3);
            listener.onResponse(new ThresholdingResult(0, 1.0d, rcfScore));
            return null;
        }).when(normalModelManager).getThresholdingResult(any(String.class), any(String.class), anyDouble(), any(ActionListener.class));

        normalModelPartitioner = mock(ModelPartitioner.class);
        rcfModelIDPrefix = "123-rcf-";
        when(normalModelPartitioner.getRcfModelId(any(String.class), anyInt())).thenAnswer(new Answer<String>() {

            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                Integer partition = (Integer) invocation.getArgument(1);
                return rcfModelIDPrefix + partition.toString();
            }

        });
        thresholdModelID = "123-threshold";
        when(normalModelPartitioner.getThresholdModelId(any(String.class))).thenReturn(thresholdModelID);
        adCircuitBreakerService = mock(ADCircuitBreakerService.class);
        when(adCircuitBreakerService.isOpen()).thenReturn(false);

        ThreadPool threadPool = mock(ThreadPool.class);
        client = mock(Client.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.threadPool().getThreadContext()).thenReturn(threadContext);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 2);

            IndexRequest request = null;
            ActionListener<IndexResponse> listener = null;
            if (args[0] instanceof IndexRequest) {
                request = (IndexRequest) args[0];
            }
            if (args[1] instanceof ActionListener) {
                listener = (ActionListener<IndexResponse>) args[1];
            }

            assertTrue(request != null && listener != null);
            ShardId shardId = new ShardId(new Index(CommonName.ANOMALY_RESULT_INDEX_ALIAS, randomAlphaOfLength(10)), 0);
            listener.onResponse(new IndexResponse(shardId, randomAlphaOfLength(10), request.id(), 1, 1, 1, true));

            return null;
        }).when(client).index(any(), any());

        indexNameResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));

        Map<String, ADStat<?>> statsMap = new HashMap<String, ADStat<?>>() {
            {
                put(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
                put(StatNames.AD_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
                put(StatNames.AD_HC_EXECUTE_REQUEST_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
                put(StatNames.AD_HC_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
            }
        };

        adStats = new ADStats(statsMap);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            if (request.index().equals(CommonName.DETECTION_STATE_INDEX)) {

                DetectorInternalState.Builder result = new DetectorInternalState.Builder().lastUpdateTime(Instant.now());

                listener
                    .onResponse(TestHelpers.createGetResponse(result.build(), detector.getDetectorId(), CommonName.DETECTION_STATE_INDEX));

            }

            return null;
        }).when(client).get(any(), any());
    }

    @Override
    @After
    public final void tearDown() throws Exception {
        tearDownTestNodes();
        client = null;
        super.tearDownLog4jForJUnit();
        super.tearDown();
    }

    private Throwable assertException(PlainActionFuture<AnomalyResultResponse> listener, Class<? extends Exception> exceptionType) {
        return expectThrows(exceptionType, () -> listener.actionGet());
    }

    public void testNormal() throws IOException {

        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            normalModelManager,
            adCircuitBreakerService,
            hashRing
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
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
            threadPool,
            NamedXContentRegistry.EMPTY
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertAnomalyResultResponse(response, 0, 1, 0d);
    }

    private void assertAnomalyResultResponse(AnomalyResultResponse response, double anomalyGrade, double confidence, double featureData) {
        assertEquals(anomalyGrade, response.getAnomalyGrade(), 0.001);
        assertEquals(confidence, response.getConfidence(), 0.001);
        assertEquals(1, response.getFeatures().size());
        FeatureData responseFeature = response.getFeatures().get(0);
        assertEquals(featureData, responseFeature.getData(), 0.001);
        assertEquals(featureId, responseFeature.getFeatureId());
        assertEquals(featureName, responseFeature.getFeatureName());
    }

    /**
     * Create handler that would return a failure
     * @param handler callback handler
     * @return handler that would return a failure
     */
    private <T extends TransportResponse> TransportResponseHandler<T> rcfFailureHandler(
        TransportResponseHandler<T> handler,
        Exception exception
    ) {
        return new TransportResponseHandler<T>() {
            @Override
            public T read(StreamInput in) throws IOException {
                return handler.read(in);
            }

            @Override
            public void handleResponse(T response) {
                handler.handleException(new RemoteTransportException("test", exception));
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

    public void noModelExceptionTemplate(
        Exception thrownException,
        String adID,
        Class<? extends Exception> expectedExceptionType,
        String error
    ) {

        TransportInterceptor failureTransportInterceptor = new TransportInterceptor() {
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
                        if (RCFResultAction.NAME.equals(action)) {
                            sender.sendRequest(connection, action, request, options, rcfFailureHandler(handler, thrownException));
                        } else {
                            sender.sendRequest(connection, action, request, options, handler);
                        }
                    }
                };
            }
        };

        // need to close nodes created in the setUp nodes and create new nodes
        // for the failure interceptor. Otherwise, we will get thread leak error.
        tearDownTestNodes();
        setupTestNodes(
            failureTransportInterceptor,
            Settings.EMPTY,
            AnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY,
            AnomalyDetectorSettings.PAGE_SIZE
        );

        // mock hashing ring response. This has to happen after setting up test nodes with the failure interceptor
        Optional<DiscoveryNode> discoveryNode = Optional.of(testNodes[1].discoveryNode());
        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeJob(any(String.class))).thenReturn(discoveryNode);
        when(hashRing.getNodeByAddress(any(TransportAddress.class))).thenReturn(discoveryNode);
        // register handler on testNodes[1]
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            testNodes[1].transportService,
            normalModelManager,
            adCircuitBreakerService,
            hashRing
        );

        TransportService realTransportService = testNodes[0].transportService;
        ClusterService realClusterService = testNodes[0].clusterService;

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
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
            NamedXContentRegistry.EMPTY
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        Throwable exception = assertException(listener, expectedExceptionType);
        assertTrue("actual message: " + exception.getMessage(), exception.getMessage().contains(error));
    }

    public void noModelExceptionTemplate(Exception exception, String adID, String error) {
        noModelExceptionTemplate(exception, adID, exception.getClass(), error);
    }

    public void testNormalColdStart() {
        noModelExceptionTemplate(
            new ResourceNotFoundException(adID, ""),
            adID,
            InternalFailure.class,
            AnomalyResultTransportAction.NO_MODEL_ERR_MSG
        );
    }

    public void testNormalColdStartRemoteException() {
        noModelExceptionTemplate(
            new NotSerializableExceptionWrapper(new ResourceNotFoundException(adID, "")),
            adID,
            AnomalyDetectionException.class,
            AnomalyResultTransportAction.NO_MODEL_ERR_MSG
        );
    }

    public void testNullPointerExceptionWhenRCF() {
        noModelExceptionTemplate(new NullPointerException(), adID, EndRunException.class, CommonErrorMessages.BUG_RESPONSE);
    }

    public void testADExceptionWhenColdStart() {
        String error = "blah";
        when(stateManager.fetchExceptionAndClear(any(String.class))).thenReturn(Optional.of(new AnomalyDetectionException(adID, error)));

        noModelExceptionTemplate(new ResourceNotFoundException(adID, ""), adID, AnomalyDetectionException.class, error);
    }

    @SuppressWarnings("unchecked")
    public void testInsufficientCapacityExceptionDuringColdStart() {

        ModelManager rcfManager = mock(ModelManager.class);
        doThrow(ResourceNotFoundException.class)
            .when(rcfManager)
            .getRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));

        when(stateManager.fetchExceptionAndClear(any(String.class)))
            .thenReturn(Optional.of(new LimitExceededException(adID, CommonErrorMessages.MEMORY_LIMIT_EXCEEDED_ERR_MSG)));

        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            rcfManager,
            adCircuitBreakerService,
            hashRing
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
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
            threadPool,
            NamedXContentRegistry.EMPTY
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, LimitExceededException.class);
    }

    @SuppressWarnings("unchecked")
    public void testInsufficientCapacityExceptionDuringRestoringModel() {

        ModelManager rcfManager = mock(ModelManager.class);
        doThrow(new NotSerializableExceptionWrapper(new LimitExceededException(adID, CommonErrorMessages.MEMORY_LIMIT_EXCEEDED_ERR_MSG)))
            .when(rcfManager)
            .getRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));

        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            rcfManager,
            adCircuitBreakerService,
            hashRing
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
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
            threadPool,
            NamedXContentRegistry.EMPTY
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, LimitExceededException.class);
    }

    private <T extends TransportResponse> TransportResponseHandler<T> rcfResponseHandler(TransportResponseHandler<T> handler) {
        return new TransportResponseHandler<T>() {
            @Override
            public T read(StreamInput in) throws IOException {
                return handler.read(in);
            }

            @Override
            @SuppressWarnings("unchecked")
            public void handleResponse(T response) {
                handler.handleResponse((T) new RCFResultResponse(1, 1, 100, new double[0], randomInt(), Version.CURRENT));
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

    public void thresholdExceptionTestTemplate(
        Exception thrownException,
        String adID,
        Class<? extends Exception> expectedExceptionType,
        String error
    ) {

        TransportInterceptor failureTransportInterceptor = new TransportInterceptor() {
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
                        if (ThresholdResultAction.NAME.equals(action)) {
                            sender.sendRequest(connection, action, request, options, rcfFailureHandler(handler, thrownException));
                        } else if (RCFResultAction.NAME.equals(action)) {
                            sender.sendRequest(connection, action, request, options, rcfResponseHandler(handler));
                        } else {
                            sender.sendRequest(connection, action, request, options, handler);
                        }
                    }
                };
            }
        };

        // need to close nodes created in the setUp nodes and create new nodes
        // for the failure interceptor. Otherwise, we will get thread leak error.
        tearDownTestNodes();
        setupTestNodes(
            failureTransportInterceptor,
            Settings.EMPTY,
            AnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY,
            AnomalyDetectorSettings.PAGE_SIZE
        );

        // mock hashing ring response. This has to happen after setting up test nodes with the failure interceptor
        Optional<DiscoveryNode> discoveryNode = Optional.of(testNodes[1].discoveryNode());
        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeJob(any(String.class))).thenReturn(discoveryNode);
        when(hashRing.getNodeByAddress(any(TransportAddress.class))).thenReturn(discoveryNode);
        // register handlers on testNodes[1]
        ActionFilters actionFilters = new ActionFilters(Collections.emptySet());
        new RCFResultTransportAction(actionFilters, testNodes[1].transportService, normalModelManager, adCircuitBreakerService, hashRing);
        new ThresholdResultTransportAction(actionFilters, testNodes[1].transportService, normalModelManager);

        TransportService realTransportService = testNodes[0].transportService;
        ClusterService realClusterService = testNodes[0].clusterService;

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
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
            NamedXContentRegistry.EMPTY
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        Throwable exception = assertException(listener, expectedExceptionType);
        assertTrue("actual message: " + exception.getMessage(), exception.getMessage().contains(error));
    }

    public void testThresholdException() {
        thresholdExceptionTestTemplate(new NullPointerException(), adID, EndRunException.class, CommonErrorMessages.BUG_RESPONSE);
    }

    public void testCircuitBreaker() {

        ADCircuitBreakerService breakerService = mock(ADCircuitBreakerService.class);
        when(breakerService.isOpen()).thenReturn(true);

        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            normalModelManager,
            breakerService,
            hashRing
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
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
            breakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, LimitExceededException.class);
    }

    /**
     * Test whether we can handle NodeNotConnectedException when sending requests to
     * remote nodes.
     *
     * @param isRCF             whether RCF model node throws node connection
     *                          exception or not
     * @param temporary         whether node has only temporary connection issue. If
     *                          yes, we should not trigger hash ring rebuilding.
     * @param numberOfBuildCall the number of expected hash ring build call
     */
    private void nodeNotConnectedExceptionTemplate(boolean isRCF, boolean temporary, int numberOfBuildCall) {
        ClusterService hackedClusterService = spy(clusterService);

        TransportService exceptionTransportService = spy(transportService);

        DiscoveryNode rcfNode = clusterService.state().nodes().getLocalNode();
        DiscoveryNode thresholdNode = testNodes[1].discoveryNode();

        if (isRCF) {
            doThrow(new NodeNotConnectedException(rcfNode, "rcf node not connected"))
                .when(exceptionTransportService)
                .getConnection(same(rcfNode));
        } else {
            when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeJob(eq(thresholdModelID))).thenReturn(Optional.of(thresholdNode));
            when(hashRing.getNodeByAddress(any())).thenReturn(Optional.of(thresholdNode));
            doThrow(new NodeNotConnectedException(rcfNode, "rcf node not connected"))
                .when(exceptionTransportService)
                .getConnection(same(thresholdNode));
        }

        if (!temporary) {
            when(hackedClusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).build());
        }

        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            exceptionTransportService,
            normalModelManager,
            adCircuitBreakerService,
            hashRing
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), exceptionTransportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            exceptionTransportService,
            settings,
            client,
            stateManager,
            featureQuery,
            normalModelManager,
            normalModelPartitioner,
            hashRing,
            hackedClusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, AnomalyDetectionException.class);

        if (!temporary) {
            verify(hashRing, times(numberOfBuildCall)).buildCirclesOnAdVersionsDirectly();
            verify(stateManager, never()).addPressure(any(String.class), any(String.class));
        } else {
            verify(hashRing, never()).buildCirclesOnAdVersionsDirectly();
            // expect 2 times since we have 2 RCF model partitions
            verify(stateManager, times(numberOfBuildCall)).addPressure(any(String.class), any(String.class));
        }
    }

    public void testRCFNodeNotConnectedException() {
        // we expect two hashRing.build calls since we have two RCF model partitions and
        // both of them returns node not connected exception
        nodeNotConnectedExceptionTemplate(true, false, 2);
    }

    public void testTemporaryRCFNodeNotConnectedException() {
        // we expect two backpressure incrementBackpressureCounter calls since we have
        // two RCF model partitions and both of them returns node not connected
        // exception
        nodeNotConnectedExceptionTemplate(true, true, 2);
    }

    public void testThresholdNodeNotConnectedException() {
        // we expect one hashRing.build calls since we have one threshold model
        // partition
        nodeNotConnectedExceptionTemplate(false, false, 1);
    }

    public void testTemporaryThresholdNodeNotConnectedException() {
        // we expect one backpressure incrementBackpressureCounter call since we have
        // one threshold model partition
        nodeNotConnectedExceptionTemplate(false, true, 1);
    }

    @SuppressWarnings("unchecked")
    public void testMute() {
        NodeStateManager muteStateManager = mock(NodeStateManager.class);
        when(muteStateManager.isMuted(any(String.class), any(String.class))).thenReturn(true);
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(muteStateManager).getAnomalyDetector(any(String.class), any(ActionListener.class));
        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            muteStateManager,
            featureQuery,
            normalModelManager,
            normalModelPartitioner,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY
        );
        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        Throwable exception = assertException(listener, AnomalyDetectionException.class);
        assertThat(exception.getMessage(), containsString(AnomalyResultTransportAction.NODE_UNRESPONSIVE_ERR_MSG));
    }

    public void alertingRequestTemplate(boolean anomalyResultIndexExists) throws IOException {
        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            normalModelManager,
            adCircuitBreakerService,
            hashRing
        );
        Optional<DiscoveryNode> localNode = Optional.of(clusterService.state().nodes().getLocalNode());

        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeJob(any(String.class))).thenReturn(localNode);
        doReturn(localNode).when(hashRing).getNodeByAddress(any());
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        new AnomalyResultTransportAction(
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
            threadPool,
            NamedXContentRegistry.EMPTY
        );

        TransportRequestOptions option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.STATE)
            .withTimeout(6000)
            .build();

        transportService
            .sendRequest(
                clusterService.state().nodes().getLocalNode(),
                AnomalyResultAction.NAME,
                new AnomalyResultRequest(adID, 100, 200),
                option,
                new TransportResponseHandler<AnomalyResultResponse>() {

                    @Override
                    public AnomalyResultResponse read(StreamInput in) throws IOException {
                        return new AnomalyResultResponse(in);
                    }

                    @Override
                    public void handleResponse(AnomalyResultResponse response) {
                        assertAnomalyResultResponse(response, 0, 1, 0d);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        assertThat(exp, is(nullValue()));
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }
                }
            );
    }

    public void testAlertingRequestWithoutResultIndex() throws IOException {
        alertingRequestTemplate(false);
    }

    public void testAlertingRequestWithResultIndex() throws IOException {
        alertingRequestTemplate(true);
    }

    public void testSerialzationResponse() throws IOException {
        AnomalyResultResponse response = new AnomalyResultResponse(
            4,
            0.993,
            1.01,
            Collections.singletonList(new FeatureData(featureId, featureName, 0d))
        );
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        AnomalyResultResponse readResponse = AnomalyResultAction.INSTANCE.getResponseReader().read(streamInput);
        assertAnomalyResultResponse(readResponse, readResponse.getAnomalyGrade(), readResponse.getConfidence(), 0d);
    }

    public void testJsonResponse() throws IOException, JsonPathNotFoundException {
        AnomalyResultResponse response = new AnomalyResultResponse(
            4,
            0.993,
            1.01,
            Collections.singletonList(new FeatureData(featureId, featureName, 0d))
        );
        XContentBuilder builder = jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = Strings.toString(builder);
        Function<JsonElement, FeatureData> function = (s) -> {
            try {
                String featureId = JsonDeserializer.getTextValue(s, FeatureData.FEATURE_ID_FIELD);
                String featureName = JsonDeserializer.getTextValue(s, FeatureData.FEATURE_NAME_FIELD);
                double featureValue = JsonDeserializer.getDoubleValue(s, FeatureData.DATA_FIELD);
                return new FeatureData(featureId, featureName, featureValue);
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            return null;
        };

        AnomalyResultResponse readResponse = new AnomalyResultResponse(
            JsonDeserializer.getDoubleValue(json, AnomalyResultResponse.ANOMALY_GRADE_JSON_KEY),
            JsonDeserializer.getDoubleValue(json, AnomalyResultResponse.CONFIDENCE_JSON_KEY),
            JsonDeserializer.getDoubleValue(json, AnomalyResultResponse.ANOMALY_SCORE_JSON_KEY),
            JsonDeserializer.getListValue(json, function, AnomalyResultResponse.FEATURES_JSON_KEY)
        );
        assertAnomalyResultResponse(readResponse, readResponse.getAnomalyGrade(), readResponse.getConfidence(), 0d);
    }

    public void testSerialzationRequest() throws IOException {
        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        AnomalyResultRequest readRequest = new AnomalyResultRequest(streamInput);
        assertThat(request.getAdID(), equalTo(readRequest.getAdID()));
        assertThat(request.getStart(), equalTo(readRequest.getStart()));
        assertThat(request.getEnd(), equalTo(readRequest.getEnd()));
    }

    public void testJsonRequest() throws IOException, JsonPathNotFoundException {
        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = Strings.toString(builder);
        assertEquals(JsonDeserializer.getTextValue(json, CommonName.ID_JSON_KEY), request.getAdID());
        assertEquals(JsonDeserializer.getLongValue(json, CommonName.START_JSON_KEY), request.getStart());
        assertEquals(JsonDeserializer.getLongValue(json, CommonName.END_JSON_KEY), request.getEnd());
    }

    public void testEmptyID() {
        ActionRequestValidationException e = new AnomalyResultRequest("", 100, 200).validate();
        assertThat(e.validationErrors(), hasItem(CommonErrorMessages.AD_ID_MISSING_MSG));
    }

    public void testZeroStartTime() {
        ActionRequestValidationException e = new AnomalyResultRequest(adID, 0, 200).validate();
        assertThat(e.validationErrors(), hasItem(startsWith(CommonErrorMessages.INVALID_TIMESTAMP_ERR_MSG)));
    }

    public void testNegativeEndTime() {
        ActionRequestValidationException e = new AnomalyResultRequest(adID, 0, -200).validate();
        assertThat(e.validationErrors(), hasItem(startsWith(CommonErrorMessages.INVALID_TIMESTAMP_ERR_MSG)));
    }

    public void testNegativeTime() {
        ActionRequestValidationException e = new AnomalyResultRequest(adID, 10, -200).validate();
        assertThat(e.validationErrors(), hasItem(startsWith(CommonErrorMessages.INVALID_TIMESTAMP_ERR_MSG)));
    }

    // no exception should be thrown
    public void testOnFailureNull() throws IOException {
        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
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
            threadPool,
            NamedXContentRegistry.EMPTY
        );
        AnomalyResultTransportAction.RCFActionListener listener = action.new RCFActionListener(
            null, null, null, null, null, null, null, null, null, 0, new AtomicInteger(), null, 1
        );
        listener.onFailure(null);
    }

    @SuppressWarnings("unchecked")
    private void setUpColdStart(ThreadPool mockThreadPool, boolean coldStartRunning) {
        SinglePointFeatures mockSinglePoint = mock(SinglePointFeatures.class);

        when(mockSinglePoint.getProcessedFeatures()).thenReturn(Optional.empty());

        doAnswer(invocation -> {
            ActionListener<SinglePointFeatures> listener = invocation.getArgument(3);
            listener.onResponse(mockSinglePoint);
            return null;
        }).when(featureQuery).getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(Boolean.FALSE);
            return null;
        }).when(stateManager).getDetectorCheckpoint(any(String.class), any(ActionListener.class));

        when(stateManager.isColdStartRunning(any(String.class))).thenReturn(coldStartRunning);

        setUpADThreadPool(mockThreadPool);
    }

    @SuppressWarnings("unchecked")
    public void testColdStartNoTrainingData() throws Exception {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(mockThreadPool, false);

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.empty());
            return null;
        }).when(featureQuery).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
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
            NamedXContentRegistry.EMPTY
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        verify(stateManager, times(1)).setException(eq(adID), any(EndRunException.class));
        verify(stateManager, times(1)).markColdStartRunning(eq(adID));
    }

    @SuppressWarnings("unchecked")
    public void testConcurrentColdStart() throws Exception {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(mockThreadPool, true);

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.empty());
            return null;
        }).when(featureQuery).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
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
            NamedXContentRegistry.EMPTY
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        verify(stateManager, never()).setException(eq(adID), any(EndRunException.class));
        verify(stateManager, never()).markColdStartRunning(eq(adID));
    }

    @SuppressWarnings("unchecked")
    public void testColdStartTimeoutPutCheckpoint() throws Exception {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(mockThreadPool, false);

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(new double[][] { { 1.0 } }));
            return null;
        }).when(featureQuery).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(2);
            listener.onFailure(new OpenSearchTimeoutException(""));
            return null;
        }).when(normalModelManager).trainModel(any(AnomalyDetector.class), any(double[][].class), any(ActionListener.class));

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
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
            NamedXContentRegistry.EMPTY
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        verify(stateManager, times(1)).setException(eq(adID), any(InternalFailure.class));
        verify(stateManager, times(1)).markColdStartRunning(eq(adID));
    }

    @SuppressWarnings("unchecked")
    public void testColdStartIllegalArgumentException() throws Exception {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(mockThreadPool, false);

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(new double[][] { { 1.0 } }));
            return null;
        }).when(featureQuery).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(2);
            listener.onFailure(new IllegalArgumentException(""));
            return null;
        }).when(normalModelManager).trainModel(any(AnomalyDetector.class), any(double[][].class), any(ActionListener.class));

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
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
            NamedXContentRegistry.EMPTY
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        verify(stateManager, times(1)).setException(eq(adID), any(EndRunException.class));
        verify(stateManager, times(1)).markColdStartRunning(eq(adID));
    }

    enum FeatureTestMode {
        FEATURE_NOT_AVAILABLE,
        ILLEGAL_STATE,
        AD_EXCEPTION
    }

    @SuppressWarnings("unchecked")
    public void featureTestTemplate(FeatureTestMode mode) throws IOException {
        if (mode == FeatureTestMode.FEATURE_NOT_AVAILABLE) {
            doAnswer(invocation -> {
                ActionListener<SinglePointFeatures> listener = invocation.getArgument(3);
                listener.onResponse(new SinglePointFeatures(Optional.empty(), Optional.empty()));
                return null;
            }).when(featureQuery).getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), any(ActionListener.class));
        } else if (mode == FeatureTestMode.ILLEGAL_STATE) {
            doThrow(IllegalArgumentException.class)
                .when(featureQuery)
                .getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), any(ActionListener.class));
        } else if (mode == FeatureTestMode.AD_EXCEPTION) {
            doThrow(AnomalyDetectionException.class)
                .when(featureQuery)
                .getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), any(ActionListener.class));
        }

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
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
            threadPool,
            NamedXContentRegistry.EMPTY
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        if (mode == FeatureTestMode.FEATURE_NOT_AVAILABLE) {
            AnomalyResultResponse response = listener.actionGet();
            assertEquals(Double.NaN, response.getAnomalyGrade(), 0.001);
            assertEquals(Double.NaN, response.getConfidence(), 0.001);
            assertEquals(Double.NaN, response.getAnomalyScore(), 0.001);
            assertThat(response.getFeatures(), is(empty()));
        } else if (mode == FeatureTestMode.ILLEGAL_STATE || mode == FeatureTestMode.AD_EXCEPTION) {
            assertException(listener, InternalFailure.class);
        }
    }

    public void testFeatureNotAvailable() throws IOException {
        featureTestTemplate(FeatureTestMode.FEATURE_NOT_AVAILABLE);
    }

    public void testFeatureIllegalState() throws IOException {
        featureTestTemplate(FeatureTestMode.ILLEGAL_STATE);
    }

    public void testFeatureAnomalyException() throws IOException {
        featureTestTemplate(FeatureTestMode.AD_EXCEPTION);
    }

    enum BlockType {
        INDEX_BLOCK,
        GLOBAL_BLOCK_WRITE,
        GLOBAL_BLOCK_READ
    }

    private void globalBlockTemplate(BlockType type, String errLogMsg, Settings indexSettings, String indexName) {
        ClusterState blockedClusterState = null;

        switch (type) {
            case GLOBAL_BLOCK_WRITE:
                blockedClusterState = ClusterState
                    .builder(new ClusterName("test cluster"))
                    .blocks(ClusterBlocks.builder().addGlobalBlock(IndexMetadata.INDEX_WRITE_BLOCK))
                    .build();
                break;
            case GLOBAL_BLOCK_READ:
                blockedClusterState = ClusterState
                    .builder(new ClusterName("test cluster"))
                    .blocks(ClusterBlocks.builder().addGlobalBlock(IndexMetadata.INDEX_READ_BLOCK))
                    .build();
                break;
            case INDEX_BLOCK:
                blockedClusterState = createIndexBlockedState(indexName, indexSettings, null);
                break;
            default:
                break;
        }

        ClusterService hackedClusterService = spy(clusterService);
        when(hackedClusterService.state()).thenReturn(blockedClusterState);

        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            normalModelManager,
            adCircuitBreakerService,
            hashRing
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            stateManager,
            featureQuery,
            normalModelManager,
            normalModelPartitioner,
            hashRing,
            hackedClusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, AnomalyDetectionException.class, errLogMsg);
    }

    private void globalBlockTemplate(BlockType type, String errLogMsg) {
        globalBlockTemplate(type, errLogMsg, null, null);
    }

    public void testReadBlock() {
        globalBlockTemplate(BlockType.GLOBAL_BLOCK_READ, AnomalyResultTransportAction.READ_WRITE_BLOCKED);
    }

    public void testWriteBlock() {
        globalBlockTemplate(BlockType.GLOBAL_BLOCK_WRITE, AnomalyResultTransportAction.READ_WRITE_BLOCKED);
    }

    public void testIndexReadBlock() {
        globalBlockTemplate(
            BlockType.INDEX_BLOCK,
            AnomalyResultTransportAction.INDEX_READ_BLOCKED,
            Settings.builder().put(IndexMetadata.INDEX_BLOCKS_READ_SETTING.getKey(), true).build(),
            "test1"
        );
    }

    public void testNullRCFResult() {
        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
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
            threadPool,
            NamedXContentRegistry.EMPTY
        );
        AnomalyResultTransportAction.RCFActionListener listener = action.new RCFActionListener(
            null, "123-rcf-0", null, "123", null, null, null, null, null, 0, new AtomicInteger(), null, 1
        );
        listener.onResponse(null);
        assertTrue(testAppender.containsMessage(AnomalyResultTransportAction.NULL_RESPONSE));
    }

    @SuppressWarnings("unchecked")
    public void testAllFeaturesDisabled() throws IOException {
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(1);
            listener.onFailure(new EndRunException(adID, CommonErrorMessages.ALL_FEATURES_DISABLED_ERR_MSG, true));
            return null;
        }).when(stateManager).getAnomalyDetector(any(String.class), any(ActionListener.class));

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
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
            threadPool,
            NamedXContentRegistry.EMPTY
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, EndRunException.class, CommonErrorMessages.ALL_FEATURES_DISABLED_ERR_MSG);
    }

    @SuppressWarnings("unchecked")
    public void testEndRunDueToNoTrainingData() {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(mockThreadPool, false);

        ModelManager rcfManager = mock(ModelManager.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<RcfResult> listener = (ActionListener<RcfResult>) args[3];
            listener.onFailure(new IndexNotFoundException(CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(rcfManager).getRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));

        when(stateManager.fetchExceptionAndClear(any(String.class)))
            .thenReturn(Optional.of(new EndRunException(adID, "Cannot get training data", false)));

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(new double[][] { { 1.0 } }));
            return null;
        }).when(featureQuery).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<Optional<Void>> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(normalModelManager).trainModel(any(AnomalyDetector.class), any(double[][].class), any(ActionListener.class));

        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            rcfManager,
            adCircuitBreakerService,
            hashRing
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
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
            NamedXContentRegistry.EMPTY
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, EndRunException.class);
        verify(stateManager, times(1)).markColdStartRunning(eq(adID));
    }

    public void testRCFNodeCircuitBreakerBroken() {
        ADCircuitBreakerService brokenCircuitBreaker = mock(ADCircuitBreakerService.class);
        when(brokenCircuitBreaker.isOpen()).thenReturn(true);

        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            normalModelManager,
            brokenCircuitBreaker,
            hashRing
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
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
            threadPool,
            NamedXContentRegistry.EMPTY
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, LimitExceededException.class, CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG);
    }

}
