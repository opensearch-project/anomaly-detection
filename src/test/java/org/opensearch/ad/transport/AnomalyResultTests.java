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

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.anyLong;
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
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.Version;
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
import org.opensearch.ad.ml.SingleStreamModelIdMapper;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorInternalState;
import org.opensearch.ad.model.FeatureData;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStat;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.stats.StatNames;
import org.opensearch.ad.stats.suppliers.CounterSupplier;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.util.SecurityClientUtil;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NotSerializableExceptionWrapper;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.NodeNotConnectedException;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
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
    private Client client;
    private SecurityClientUtil clientUtil;
    private AnomalyDetector detector;
    private HashRing hashRing;
    private IndexNameExpressionResolver indexNameResolver;
    private String thresholdModelID;
    private String adID;
    private String featureId;
    private String featureName;
    private ADCircuitBreakerService adCircuitBreakerService;
    private ADStats adStats;
    private double confidence;
    private double anomalyGrade;
    private ADTaskManager adTaskManager;

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
        when(detector.getDetectorIntervalInMinutes()).thenReturn(1L);

        hashRing = mock(HashRing.class);
        Optional<DiscoveryNode> localNode = Optional.of(clusterService.state().nodes().getLocalNode());
        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(any(String.class))).thenReturn(localNode);
        doReturn(localNode).when(hashRing).getNodeByAddress(any());
        featureQuery = mock(FeatureManager.class);

        doAnswer(invocation -> {
            ActionListener<SinglePointFeatures> listener = invocation.getArgument(3);
            listener.onResponse(new SinglePointFeatures(Optional.of(new double[] { 0.0d }), Optional.of(new double[] { 0 })));
            return null;
        }).when(featureQuery).getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), any(ActionListener.class));

        double rcfScore = 0.2;
        confidence = 0.91;
        anomalyGrade = 0.5;
        normalModelManager = mock(ModelManager.class);
        long totalUpdates = 1440;
        int relativeIndex = 0;
        double[] currentTimeAttribution = new double[] { 0.5, 0.5 };
        double[] pastValues = new double[] { 123, 456 };
        double[][] expectedValuesList = new double[][] { new double[] { 789, 12 } };
        double[] likelihood = new double[] { 1 };
        double threshold = 1.1d;
        doAnswer(invocation -> {
            ActionListener<ThresholdingResult> listener = invocation.getArgument(3);
            listener
                .onResponse(
                    new ThresholdingResult(
                        anomalyGrade,
                        confidence,
                        rcfScore,
                        totalUpdates,
                        relativeIndex,
                        currentTimeAttribution,
                        pastValues,
                        expectedValuesList,
                        likelihood,
                        threshold,
                        30
                    )
                );
            return null;
        }).when(normalModelManager).getTRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<ThresholdingResult> listener = invocation.getArgument(3);
            listener.onResponse(new ThresholdingResult(0, 1.0d, rcfScore));
            return null;
        }).when(normalModelManager).getThresholdingResult(any(String.class), any(String.class), anyDouble(), any(ActionListener.class));

        thresholdModelID = SingleStreamModelIdMapper.getThresholdModelId(adID); // "123-threshold";
        // when(normalModelPartitioner.getThresholdModelId(any(String.class))).thenReturn(thresholdModelID);
        adCircuitBreakerService = mock(ADCircuitBreakerService.class);
        when(adCircuitBreakerService.isOpen()).thenReturn(false);

        ThreadPool threadPool = mock(ThreadPool.class);
        client = mock(Client.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.threadPool().getThreadContext()).thenReturn(threadContext);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length >= 2
            );

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
            listener.onResponse(new IndexResponse(shardId, request.id(), 1, 1, 1, true));

            return null;
        }).when(client).index(any(), any());
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        clientUtil = new SecurityClientUtil(nodeStateManager, settings);

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

        adTaskManager = mock(ADTaskManager.class);
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(3);
            listener.onResponse(true);
            return null;
        })
            .when(adTaskManager)
            .initRealtimeTaskCacheAndCleanupStaleCache(
                anyString(),
                any(AnomalyDetector.class),
                any(TransportService.class),
                any(ActionListener.class)
            );
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
            hashRing,
            adStats
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertAnomalyResultResponse(response, anomalyGrade, confidence, 0d);
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
        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(any(String.class))).thenReturn(discoveryNode);
        when(hashRing.getNodeByAddress(any(TransportAddress.class))).thenReturn(discoveryNode);
        // register handler on testNodes[1]
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            testNodes[1].transportService,
            normalModelManager,
            adCircuitBreakerService,
            hashRing,
            adStats
        );

        TransportService realTransportService = testNodes[0].transportService;
        ClusterService realClusterService = testNodes[0].clusterService;

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            realTransportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            realClusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
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

    @SuppressWarnings("unchecked")
    public void testInsufficientCapacityExceptionDuringColdStart() {

        ModelManager rcfManager = mock(ModelManager.class);
        doThrow(ResourceNotFoundException.class)
            .when(rcfManager)
            .getTRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));

        when(stateManager.fetchExceptionAndClear(any(String.class)))
            .thenReturn(Optional.of(new LimitExceededException(adID, CommonErrorMessages.MEMORY_LIMIT_EXCEEDED_ERR_MSG)));

        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            rcfManager,
            adCircuitBreakerService,
            hashRing,
            adStats
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
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
            .getTRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));

        // These constructors register handler in transport service
        new RCFResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            rcfManager,
            adCircuitBreakerService,
            hashRing,
            adStats
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
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
                handler
                    .handleResponse(
                        (T) new RCFResultResponse(
                            1,
                            1,
                            100,
                            new double[0],
                            randomInt(),
                            randomDouble(),
                            Version.CURRENT,
                            randomIntBetween(-3, 0),
                            new double[] { randomDouble(), randomDouble() },
                            new double[][] { new double[] { randomDouble(), randomDouble() } },
                            new double[] { randomDouble() },
                            randomDoubleBetween(1.1, 10.0, true)
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
        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(any(String.class))).thenReturn(discoveryNode);
        when(hashRing.getNodeByAddress(any(TransportAddress.class))).thenReturn(discoveryNode);
        // register handlers on testNodes[1]
        ActionFilters actionFilters = new ActionFilters(Collections.emptySet());
        new RCFResultTransportAction(
            actionFilters,
            testNodes[1].transportService,
            normalModelManager,
            adCircuitBreakerService,
            hashRing,
            adStats
        );
        new ThresholdResultTransportAction(actionFilters, testNodes[1].transportService, normalModelManager);

        TransportService realTransportService = testNodes[0].transportService;
        ClusterService realClusterService = testNodes[0].clusterService;

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            realTransportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            realClusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        Throwable exception = assertException(listener, expectedExceptionType);
        assertTrue("actual message: " + exception.getMessage(), exception.getMessage().contains(error));
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
            hashRing,
            adStats
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            breakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
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
            when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(eq(thresholdModelID))).thenReturn(Optional.of(thresholdNode));
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
            hashRing,
            adStats
        );

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            exceptionTransportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            hackedClusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, AnomalyDetectionException.class);

        if (!temporary) {
            verify(hashRing, times(numberOfBuildCall)).buildCirclesForRealtimeAD();
            verify(stateManager, never()).addPressure(any(String.class), any(String.class));
        } else {
            verify(hashRing, never()).buildCirclesForRealtimeAD();
            verify(stateManager, times(numberOfBuildCall)).addPressure(any(String.class), any(String.class));
        }
    }

    public void testRCFNodeNotConnectedException() {
        // we expect one hashRing.build calls since we have one RCF model partitions
        nodeNotConnectedExceptionTemplate(true, false, 1);
    }

    public void testTemporaryRCFNodeNotConnectedException() {
        // we expect one hashRing.build calls since we have one RCF model partitions
        nodeNotConnectedExceptionTemplate(true, true, 1);
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
            clientUtil,
            muteStateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
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
            hashRing,
            adStats
        );
        Optional<DiscoveryNode> localNode = Optional.of(clusterService.state().nodes().getLocalNode());

        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(any(String.class))).thenReturn(localNode);
        doReturn(localNode).when(hashRing).getNodeByAddress(any());
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
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

    public void testSerialzationResponse() throws IOException {
        AnomalyResultResponse response = new AnomalyResultResponse(
            4d,
            0.993,
            1.01,
            Collections.singletonList(new FeatureData(featureId, featureName, 0d)),
            randomAlphaOfLength(4),
            randomLong(),
            randomLong(),
            randomBoolean(),
            randomInt(),
            new double[] { randomDoubleBetween(0, 1.0, true), randomDoubleBetween(0, 1.0, true) },
            new double[] { randomDouble(), randomDouble() },
            new double[][] { new double[] { randomDouble(), randomDouble() } },
            new double[] { randomDouble() },
            randomDoubleBetween(1.1, 10.0, true)
        );
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        AnomalyResultResponse readResponse = AnomalyResultAction.INSTANCE.getResponseReader().read(streamInput);
        assertAnomalyResultResponse(readResponse, readResponse.getAnomalyGrade(), readResponse.getConfidence(), 0d);
    }

    public void testJsonResponse() throws IOException, JsonPathNotFoundException {
        AnomalyResultResponse response = new AnomalyResultResponse(
            4d,
            0.993,
            1.01,
            Collections.singletonList(new FeatureData(featureId, featureName, 0d)),
            randomAlphaOfLength(4),
            randomLong(),
            randomLong(),
            randomBoolean(),
            randomInt(),
            new double[] { randomDoubleBetween(0, 1.0, true), randomDoubleBetween(0, 1.0, true) },
            new double[] { randomDouble(), randomDouble() },
            new double[][] { new double[] { randomDouble(), randomDouble() } },
            new double[] { randomDouble() },
            randomDoubleBetween(1.1, 10.0, true)
        );
        XContentBuilder builder = jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = builder.toString();
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
            JsonDeserializer.getListValue(json, function, AnomalyResultResponse.FEATURES_JSON_KEY),
            randomAlphaOfLength(4),
            randomLong(),
            randomLong(),
            randomBoolean(),
            randomInt(),
            new double[] { randomDoubleBetween(0, 1.0, true), randomDoubleBetween(0, 1.0, true) },
            new double[] { randomDouble(), randomDouble() },
            new double[][] { new double[] { randomDouble(), randomDouble() } },
            new double[] { randomDouble() },
            randomDoubleBetween(1.1, 10.0, true)
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

        String json = builder.toString();
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
    @SuppressWarnings("unchecked")
    public void testOnFailureNull() throws IOException {
        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
        );
        AnomalyResultTransportAction.RCFActionListener listener = action.new RCFActionListener(
            null, null, null, null, mock(ActionListener.class), null, null
        );
        listener.onFailure(null);
    }

    static class ColdStartConfig {
        boolean coldStartRunning = false;
        Exception getCheckpointException = null;

        ColdStartConfig(Builder builder) {
            this.coldStartRunning = builder.coldStartRunning;
            this.getCheckpointException = builder.getCheckpointException;
        }

        static class Builder {
            boolean coldStartRunning = false;
            Exception getCheckpointException = null;

            Builder coldStartRunning(boolean coldStartRunning) {
                this.coldStartRunning = coldStartRunning;
                return this;
            }

            Builder getCheckpointException(Exception exception) {
                this.getCheckpointException = exception;
                return this;
            }

            public ColdStartConfig build() {
                return new ColdStartConfig(this);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void setUpColdStart(ThreadPool mockThreadPool, ColdStartConfig config) {
        SinglePointFeatures mockSinglePoint = mock(SinglePointFeatures.class);

        when(mockSinglePoint.getProcessedFeatures()).thenReturn(Optional.empty());

        doAnswer(invocation -> {
            ActionListener<SinglePointFeatures> listener = invocation.getArgument(3);
            listener.onResponse(mockSinglePoint);
            return null;
        }).when(featureQuery).getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            if (config.getCheckpointException == null) {
                listener.onResponse(Boolean.FALSE);
            } else {
                listener.onFailure(config.getCheckpointException);
            }

            return null;
        }).when(stateManager).getDetectorCheckpoint(any(String.class), any(ActionListener.class));

        when(stateManager.isColdStartRunning(any(String.class))).thenReturn(config.coldStartRunning);

        setUpADThreadPool(mockThreadPool);
    }

    @SuppressWarnings("unchecked")
    public void testColdStartNoTrainingData() throws Exception {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(mockThreadPool, new ColdStartConfig.Builder().coldStartRunning(false).build());

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
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
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
        setUpColdStart(mockThreadPool, new ColdStartConfig.Builder().coldStartRunning(true).build());

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
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
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
        setUpColdStart(mockThreadPool, new ColdStartConfig.Builder().coldStartRunning(false).build());

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
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
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
        setUpColdStart(mockThreadPool, new ColdStartConfig.Builder().coldStartRunning(false).build());

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
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
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
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
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
            hashRing,
            adStats
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            hackedClusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
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

    @SuppressWarnings("unchecked")
    public void testNullRCFResult() {
        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
        );
        AnomalyResultTransportAction.RCFActionListener listener = action.new RCFActionListener(
            "123-rcf-0", null, "123", null, mock(ActionListener.class), null, null
        );
        listener.onResponse(null);
        assertTrue(testAppender.containsMessage(AnomalyResultTransportAction.NULL_RESPONSE));
    }

    @SuppressWarnings("unchecked")
    public void testNormalRCFResult() {
        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
        );
        ActionListener<AnomalyResultResponse> listener = mock(ActionListener.class);
        AnomalyResultTransportAction.RCFActionListener rcfListener = action.new RCFActionListener(
            "123-rcf-0", null, "nodeID", detector, listener, null, adID
        );
        double[] attribution = new double[] { 1. };
        long totalUpdates = 32;
        double grade = 0.5;
        ArgumentCaptor<AnomalyResultResponse> responseCaptor = ArgumentCaptor.forClass(AnomalyResultResponse.class);
        rcfListener
            .onResponse(new RCFResultResponse(0.3, 0, 26, attribution, totalUpdates, grade, Version.CURRENT, 0, null, null, null, 1.1));
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(grade, responseCaptor.getValue().getAnomalyGrade(), 1e-10);
    }

    @SuppressWarnings("unchecked")
    public void testNullPointerRCFResult() {
        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
        );
        ActionListener<AnomalyResultResponse> listener = mock(ActionListener.class);
        // detector being null causes NullPointerException
        AnomalyResultTransportAction.RCFActionListener rcfListener = action.new RCFActionListener(
            "123-rcf-0", null, "nodeID", null, listener, null, adID
        );
        double[] attribution = new double[] { 1. };
        long totalUpdates = 32;
        double grade = 0.5;
        ArgumentCaptor<Exception> failureCaptor = ArgumentCaptor.forClass(Exception.class);
        rcfListener
            .onResponse(new RCFResultResponse(0.3, 0, 26, attribution, totalUpdates, grade, Version.CURRENT, 0, null, null, null, 1.1));
        verify(listener, times(1)).onFailure(failureCaptor.capture());
        Exception failure = failureCaptor.getValue();
        assertTrue(failure instanceof InternalFailure);
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
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, EndRunException.class, CommonErrorMessages.ALL_FEATURES_DISABLED_ERR_MSG);
    }

    @SuppressWarnings("unchecked")
    public void testEndRunDueToNoTrainingData() {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(mockThreadPool, new ColdStartConfig.Builder().coldStartRunning(false).build());

        ModelManager rcfManager = mock(ModelManager.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<ThresholdingResult> listener = (ActionListener<ThresholdingResult>) args[3];
            listener.onFailure(new IndexNotFoundException(CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(rcfManager).getTRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));

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
            hashRing,
            adStats
        );
        new ThresholdResultTransportAction(new ActionFilters(Collections.emptySet()), transportService, normalModelManager);

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
        );

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        assertException(listener, EndRunException.class);
        verify(stateManager, times(1)).markColdStartRunning(eq(adID));
    }

    @SuppressWarnings({ "unchecked" })
    public void testColdStartEndRunException() {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(mockThreadPool, new ColdStartConfig.Builder().coldStartRunning(false).build());

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.empty());
            return null;
        }).when(featureQuery).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));

        when(stateManager.fetchExceptionAndClear(anyString()))
            .thenReturn(
                Optional
                    .of(
                        new EndRunException(
                            adID,
                            CommonErrorMessages.INVALID_SEARCH_QUERY_MSG,
                            new NoSuchElementException("No value present"),
                            false
                        )
                    )
            );
        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
        );

        action.doExecute(null, request, listener);
        assertException(listener, EndRunException.class, CommonErrorMessages.INVALID_SEARCH_QUERY_MSG);
        verify(featureQuery, times(1)).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));
    }

    @SuppressWarnings({ "unchecked" })
    public void testColdStartEndRunExceptionNow() {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(mockThreadPool, new ColdStartConfig.Builder().coldStartRunning(false).build());

        when(stateManager.fetchExceptionAndClear(anyString()))
            .thenReturn(
                Optional
                    .of(
                        new EndRunException(
                            adID,
                            CommonErrorMessages.INVALID_SEARCH_QUERY_MSG,
                            new NoSuchElementException("No value present"),
                            true
                        )
                    )
            );
        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
        );

        action.doExecute(null, request, listener);
        assertException(listener, EndRunException.class, CommonErrorMessages.INVALID_SEARCH_QUERY_MSG);
        verify(featureQuery, never()).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));
    }

    @SuppressWarnings({ "unchecked" })
    public void testColdStartBecauseFailtoGetCheckpoint() {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(
            mockThreadPool,
            new ColdStartConfig.Builder().getCheckpointException(new IndexNotFoundException(CommonName.CHECKPOINT_INDEX_NAME)).build()
        );

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.empty());
            return null;
        }).when(featureQuery).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
        );

        action.doExecute(null, request, listener);
        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.001);
        verify(featureQuery, times(1)).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));
    }

    @SuppressWarnings({ "unchecked" })
    public void testNoColdStartDueToUnknownException() {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(mockThreadPool, new ColdStartConfig.Builder().getCheckpointException(new RuntimeException()).build());

        doAnswer(invocation -> {
            ActionListener<Optional<double[][]>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.empty());
            return null;
        }).when(featureQuery).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));

        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
        );

        action.doExecute(null, request, listener);
        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.001);
        verify(featureQuery, never()).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));
    }
}
