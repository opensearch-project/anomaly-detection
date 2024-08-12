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
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.timeseries.TestHelpers.createIndexBlockedState;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.common.exception.JsonPathNotFoundException;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.ml.ADRealTimeInferencer;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorInternalState;
import org.opensearch.ad.ratelimit.ADCheckpointReadWorker;
import org.opensearch.ad.ratelimit.ADColdStartWorker;
import org.opensearch.ad.ratelimit.ADResultWriteRequest;
import org.opensearch.ad.ratelimit.ADResultWriteWorker;
import org.opensearch.ad.ratelimit.ADSaveResultStrategy;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.task.ADTaskManager;
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
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.InternalFailure;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.SingleStreamModelIdMapper;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.TimeSeriesStat;
import org.opensearch.timeseries.stats.suppliers.CounterSupplier;
import org.opensearch.timeseries.transport.ResultProcessor;
import org.opensearch.timeseries.transport.ResultResponse;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.NodeNotConnectedException;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import com.google.gson.JsonElement;

import test.org.opensearch.ad.util.JsonDeserializer;
import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

public class AnomalyResultTests extends AbstractTimeSeriesTest {
    private Settings settings;
    private TransportService transportService;
    private ClusterService clusterService;
    private NodeStateManager stateManager;
    private FeatureManager featureQuery;
    private ADModelManager normalModelManager;
    private Client client;
    private SecurityClientUtil clientUtil;
    private AnomalyDetector detector;
    private HashRing hashRing;
    private IndexNameExpressionResolver indexNameResolver;
    private String thresholdModelID;
    private String adID;
    private String featureId;
    private String featureName;
    private CircuitBreakerService adCircuitBreakerService;
    private ADStats adStats;
    private double confidence;
    private double anomalyGrade;
    private ADTaskManager adTaskManager;
    private ADCheckpointReadWorker checkpointReadQueue;
    private ADCacheProvider cacheProvider;
    private ADRealTimeInferencer inferencer;
    private ADColdStartWorker coldStartWorker;

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

        setupTestNodes(AnomalyDetectorSettings.AD_MAX_ENTITIES_PER_QUERY, AnomalyDetectorSettings.AD_PAGE_SIZE);

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
        when(detector.getId()).thenReturn(adID);
        when(detector.getCategoryFields()).thenReturn(null);
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(stateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));
        when(detector.getIntervalInMinutes()).thenReturn(1L);

        hashRing = mock(HashRing.class);
        Optional<DiscoveryNode> localNode = Optional.of(clusterService.state().nodes().getLocalNode());
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(any(String.class))).thenReturn(localNode);
        doReturn(localNode).when(hashRing).getNodeByAddress(any());
        featureQuery = mock(FeatureManager.class);

        doAnswer(invocation -> {
            ActionListener<Optional<double[]>> listener = invocation.getArgument(4);
            listener.onResponse(Optional.of(new double[] { 0.0d }));
            return null;
        })
            .when(featureQuery)
            .getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), eq(AnalysisType.AD), any(ActionListener.class));

        double rcfScore = 0.2;
        confidence = 0.91;
        anomalyGrade = 0.5;
        normalModelManager = mock(ADModelManager.class);
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
                        30,
                        new double[2],
                        null
                    )
                );
            return null;
        }).when(normalModelManager).getTRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<ThresholdingResult> listener = invocation.getArgument(3);
            listener.onResponse(new ThresholdingResult(0, 1.0d, rcfScore));
            return null;
        }).when(normalModelManager).getThresholdingResult(any(String.class), any(String.class), anyDouble(), any(ActionListener.class));

        when(normalModelManager.getResult(any(), any(), any(), any(), any()))
            .thenReturn(new ThresholdingResult(anomalyGrade, confidence, rcfScore));

        thresholdModelID = SingleStreamModelIdMapper.getThresholdModelId(adID); // "123-threshold";
        // when(normalModelPartitioner.getThresholdModelId(any(String.class))).thenReturn(thresholdModelID);
        adCircuitBreakerService = mock(CircuitBreakerService.class);
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
            ShardId shardId = new ShardId(new Index(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS, randomAlphaOfLength(10)), 0);
            listener.onResponse(new IndexResponse(shardId, request.id(), 1, 1, 1, true));

            return null;
        }).when(client).index(any(), any());
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        clientUtil = new SecurityClientUtil(nodeStateManager, settings);

        indexNameResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));

        Map<String, TimeSeriesStat<?>> statsMap = new HashMap<String, TimeSeriesStat<?>>() {
            {
                put(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()));
                put(StatNames.AD_EXECUTE_FAIL_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()));
                put(StatNames.AD_HC_EXECUTE_REQUEST_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()));
                put(StatNames.AD_HC_EXECUTE_FAIL_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()));
                put(StatNames.AD_MODEL_CORRUTPION_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()));
            }
        };

        adStats = new ADStats(statsMap);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            if (request.index().equals(ADCommonName.DETECTION_STATE_INDEX)) {

                DetectorInternalState.Builder result = new DetectorInternalState.Builder().lastUpdateTime(Instant.now());

                listener.onResponse(TestHelpers.createGetResponse(result.build(), detector.getId(), ADCommonName.DETECTION_STATE_INDEX));
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

        checkpointReadQueue = mock(ADCheckpointReadWorker.class);

        cacheProvider = mock(ADCacheProvider.class);
        when(cacheProvider.get()).thenReturn(mock(ADPriorityCache.class));

        coldStartWorker = mock(ADColdStartWorker.class);
        inferencer = new ADRealTimeInferencer(
            normalModelManager,
            adStats,
            mock(ADCheckpointDao.class),
            coldStartWorker,
            mock(ADSaveResultStrategy.class),
            cacheProvider,
            threadPool
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

    public void testNormal() throws IOException, InterruptedException {

        ADPriorityCache adPriorityCache = mock(ADPriorityCache.class);
        when(cacheProvider.get()).thenReturn(adPriorityCache);
        when(adPriorityCache.get(anyString(), any())).thenReturn(mock(ModelState.class));

        ADResultWriteWorker resultWriteWorker = mock(ADResultWriteWorker.class);
        CountDownLatch inProgress = new CountDownLatch(1);
        doAnswer(invocation -> {
            ADResultWriteRequest request = invocation.getArgument(0);
            assertEquals(anomalyGrade, request.getResult().getAnomalyGrade(), 0.001);
            inProgress.countDown();
            return null;
        }).when(resultWriteWorker).put(any(ADResultWriteRequest.class));

        // These constructors register handler in transport service
        new ADSingleStreamResultTransportAction(
            transportService,
            new ActionFilters(Collections.emptySet()),
            adCircuitBreakerService,
            cacheProvider,
            stateManager,
            mock(ADCheckpointReadWorker.class),
            inferencer,
            threadPool
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

        inProgress.await(60, TimeUnit.SECONDS);
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
            AnomalyDetectorSettings.AD_MAX_ENTITIES_PER_QUERY,
            AnomalyDetectorSettings.AD_PAGE_SIZE
        );

        // mock hashing ring response. This has to happen after setting up test nodes with the failure interceptor
        Optional<DiscoveryNode> discoveryNode = Optional.of(testNodes[1].discoveryNode());
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(any(String.class))).thenReturn(discoveryNode);
        when(hashRing.getNodeByAddress(any(TransportAddress.class))).thenReturn(discoveryNode);
        // register handler on testNodes[1]
        new ADSingleStreamResultTransportAction(
            testNodes[1].transportService,
            new ActionFilters(Collections.emptySet()),
            adCircuitBreakerService,
            mock(ADCacheProvider.class),
            stateManager,
            mock(ADCheckpointReadWorker.class),
            inferencer,
            threadPool
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

        ADModelManager rcfManager = mock(ADModelManager.class);
        doThrow(ResourceNotFoundException.class)
            .when(rcfManager)
            .getTRcfResult(any(String.class), any(String.class), any(double[].class), any(ActionListener.class));

        when(stateManager.fetchExceptionAndClear(any(String.class)))
            .thenReturn(Optional.of(new LimitExceededException(adID, CommonMessages.MEMORY_LIMIT_EXCEEDED_ERR_MSG)));

        // These constructors register handler in transport service
        new ADSingleStreamResultTransportAction(
            transportService,
            new ActionFilters(Collections.emptySet()),
            adCircuitBreakerService,
            mock(ADCacheProvider.class),
            stateManager,
            mock(ADCheckpointReadWorker.class),
            inferencer,
            threadPool
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

    public void testInsufficientCapacityExceptionDuringRestoringModel() throws InterruptedException {
        ADModelManager badModelManager = mock(ADModelManager.class);
        doThrow(new NullPointerException()).when(badModelManager).getResult(any(), any(), any(), any(), any());

        inferencer = new ADRealTimeInferencer(
            badModelManager,
            adStats,
            mock(ADCheckpointDao.class),
            coldStartWorker,
            mock(ADSaveResultStrategy.class),
            cacheProvider,
            threadPool
        );

        ADPriorityCache adPriorityCache = mock(ADPriorityCache.class);
        when(cacheProvider.get()).thenReturn(adPriorityCache);
        when(adPriorityCache.get(anyString(), any()))
            .thenReturn(MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build()));

        CountDownLatch inProgress = new CountDownLatch(1);
        doAnswer(invocation -> {
            inProgress.countDown();
            return null;
        }).when(coldStartWorker).put(any(FeatureRequest.class));

        // These constructors register handler in transport service
        new ADSingleStreamResultTransportAction(
            transportService,
            new ActionFilters(Collections.emptySet()),
            adCircuitBreakerService,
            cacheProvider,
            stateManager,
            mock(ADCheckpointReadWorker.class),
            inferencer,
            threadPool
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
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            NamedXContentRegistry.EMPTY,
            adTaskManager
        );

        // make sure request data end time is assigned after state initialization to pass Inferencer.tryProcess method time check.
        long start = System.currentTimeMillis() - 100;
        long end = System.currentTimeMillis();
        AnomalyResultRequest request = new AnomalyResultRequest(adID, start, end);
        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        inProgress.await(30, TimeUnit.SECONDS);
        // null pointer exception caused re-cold start
        verify(coldStartWorker, times(1)).put(any(FeatureRequest.class));
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
            AnomalyDetectorSettings.AD_MAX_ENTITIES_PER_QUERY,
            AnomalyDetectorSettings.AD_PAGE_SIZE
        );

        // mock hashing ring response. This has to happen after setting up test nodes with the failure interceptor
        Optional<DiscoveryNode> discoveryNode = Optional.of(testNodes[1].discoveryNode());
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(any(String.class))).thenReturn(discoveryNode);
        when(hashRing.getNodeByAddress(any(TransportAddress.class))).thenReturn(discoveryNode);
        // register handlers on testNodes[1]
        ActionFilters actionFilters = new ActionFilters(Collections.emptySet());
        new ADSingleStreamResultTransportAction(
            testNodes[1].transportService,
            actionFilters,
            adCircuitBreakerService,
            mock(ADCacheProvider.class),
            stateManager,
            mock(ADCheckpointReadWorker.class),
            inferencer,
            threadPool
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

        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.isOpen()).thenReturn(true);

        // These constructors register handler in transport service
        new ADSingleStreamResultTransportAction(
            transportService,
            new ActionFilters(Collections.emptySet()),
            breakerService,
            mock(ADCacheProvider.class),
            stateManager,
            mock(ADCheckpointReadWorker.class),
            inferencer,
            threadPool
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
     * @throws InterruptedException
     */
    private void nodeNotConnectedExceptionTemplate(boolean isRCF, boolean temporary, int numberOfBuildCall) throws InterruptedException {
        ClusterService hackedClusterService = spy(clusterService);

        TransportService exceptionTransportService = spy(transportService);

        DiscoveryNode rcfNode = clusterService.state().nodes().getLocalNode();
        DiscoveryNode thresholdNode = testNodes[1].discoveryNode();

        CountDownLatch inProgress = new CountDownLatch(1);
        if (isRCF) {
            // when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(eq(rcfModelID))).thenReturn(Optional.of(rcfNode));
            doThrow(new NodeNotConnectedException(rcfNode, "rcf node not connected"))
                .when(exceptionTransportService)
                .getConnection(same(rcfNode));
            doAnswer(invocation -> {
                inProgress.countDown();
                return null;
            }).when(hashRing).buildCirclesForRealtime();
        } else {
            when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(eq(thresholdModelID))).thenReturn(Optional.of(thresholdNode));
            when(hashRing.getNodeByAddress(any())).thenReturn(Optional.of(thresholdNode));
            doThrow(new NodeNotConnectedException(thresholdNode, "threshold node not connected"))
                .when(exceptionTransportService)
                .getConnection(same(thresholdNode));
        }

        if (!temporary) {
            when(hackedClusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).build());
        }

        // These constructors register handler in transport service
        new ADSingleStreamResultTransportAction(
            exceptionTransportService,
            new ActionFilters(Collections.emptySet()),
            adCircuitBreakerService,
            mock(ADCacheProvider.class),
            stateManager,
            mock(ADCheckpointReadWorker.class),
            inferencer,
            threadPool
        );

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            exceptionTransportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
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

        inProgress.await(60, TimeUnit.SECONDS);
        // assertEquals(listener, TimeSeriesException.class);

        if (!temporary) {
            verify(hashRing, times(numberOfBuildCall)).buildCirclesForRealtime();
            verify(stateManager, never()).addPressure(any(String.class), any(String.class));
        } else {
            verify(hashRing, never()).buildCirclesForRealtime();
            verify(stateManager, times(numberOfBuildCall)).addPressure(any(String.class), any(String.class));
        }
    }

    public void testRCFNodeNotConnectedException() throws InterruptedException {
        // we expect one hashRing.build calls since we have one RCF model partitions
        nodeNotConnectedExceptionTemplate(true, false, 1);
    }

    public void testTemporaryRCFNodeNotConnectedException() throws InterruptedException {
        // we expect one hashRing.build calls since we have one RCF model partitions
        nodeNotConnectedExceptionTemplate(true, true, 1);
    }

    @SuppressWarnings("unchecked")
    public void testMute() {
        NodeStateManager muteStateManager = mock(NodeStateManager.class);
        when(muteStateManager.isMuted(any(String.class), any(String.class))).thenReturn(true);
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(muteStateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));
        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            muteStateManager,
            featureQuery,
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

        Throwable exception = assertException(listener, TimeSeriesException.class);
        assertThat(exception.getMessage(), containsString(ResultProcessor.NODE_UNRESPONSIVE_ERR_MSG));
    }

    public void alertingRequestTemplate(boolean anomalyResultIndexExists) throws IOException {
        // These constructors register handler in transport service
        new ADSingleStreamResultTransportAction(
            transportService,
            new ActionFilters(Collections.emptySet()),
            adCircuitBreakerService,
            mock(ADCacheProvider.class),
            stateManager,
            mock(ADCheckpointReadWorker.class),
            inferencer,
            threadPool
        );
        Optional<DiscoveryNode> localNode = Optional.of(clusterService.state().nodes().getLocalNode());

        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(any(String.class))).thenReturn(localNode);
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
        ResultResponse response = new AnomalyResultResponse(
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
            randomDoubleBetween(1.1, 10.0, true),
            null
        );
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        AnomalyResultResponse readResponse = AnomalyResultAction.INSTANCE.getResponseReader().read(streamInput);
        assertAnomalyResultResponse(readResponse, readResponse.getAnomalyGrade(), readResponse.getConfidence(), 0d);
    }

    public void testJsonResponse() throws IOException, JsonPathNotFoundException {
        ResultResponse response = new AnomalyResultResponse(
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
            randomDoubleBetween(1.1, 10.0, true),
            null
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
            randomDoubleBetween(1.1, 10.0, true),
            null
        );
        assertAnomalyResultResponse(readResponse, readResponse.getAnomalyGrade(), readResponse.getConfidence(), 0d);
    }

    public void testSerialzationRequest() throws IOException {
        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        AnomalyResultRequest readRequest = new AnomalyResultRequest(streamInput);
        assertThat(request.getConfigId(), equalTo(readRequest.getConfigId()));
        assertThat(request.getStart(), equalTo(readRequest.getStart()));
        assertThat(request.getEnd(), equalTo(readRequest.getEnd()));
    }

    public void testJsonRequest() throws IOException, JsonPathNotFoundException {
        AnomalyResultRequest request = new AnomalyResultRequest(adID, 100, 200);
        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = builder.toString();
        assertEquals(JsonDeserializer.getTextValue(json, ADCommonName.ID_JSON_KEY), request.getConfigId());
        assertEquals(JsonDeserializer.getLongValue(json, CommonName.START_JSON_KEY), request.getStart());
        assertEquals(JsonDeserializer.getLongValue(json, CommonName.END_JSON_KEY), request.getEnd());
    }

    public void testEmptyID() {
        ActionRequestValidationException e = new AnomalyResultRequest("", 100, 200).validate();
        assertThat(e.validationErrors(), hasItem(ADCommonMessages.AD_ID_MISSING_MSG));
    }

    public void testZeroStartTime() {
        ActionRequestValidationException e = new AnomalyResultRequest(adID, 0, 200).validate();
        assertThat(e.validationErrors(), hasItem(startsWith(CommonMessages.INVALID_TIMESTAMP_ERR_MSG)));
    }

    public void testNegativeEndTime() {
        ActionRequestValidationException e = new AnomalyResultRequest(adID, 0, -200).validate();
        assertThat(e.validationErrors(), hasItem(startsWith(CommonMessages.INVALID_TIMESTAMP_ERR_MSG)));
    }

    public void testNegativeTime() {
        ActionRequestValidationException e = new AnomalyResultRequest(adID, 10, -200).validate();
        assertThat(e.validationErrors(), hasItem(startsWith(CommonMessages.INVALID_TIMESTAMP_ERR_MSG)));
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

        doAnswer(invocation -> {
            ActionListener<Optional<double[]>> listener = invocation.getArgument(4);
            listener.onResponse(Optional.of(new double[] { 0.0d }));
            return null;
        })
            .when(featureQuery)
            .getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), eq(AnalysisType.AD), any(ActionListener.class));

        ADCacheProvider cacheProvider = mock(ADCacheProvider.class);
        ADPriorityCache priorityCache = mock(ADPriorityCache.class);
        when(cacheProvider.get()).thenReturn(priorityCache);
        when(priorityCache.get(any(), any())).thenReturn(null);

        // register action handler
        new ADSingleStreamResultTransportAction(
            transportService,
            new ActionFilters(Collections.emptySet()),
            adCircuitBreakerService,
            cacheProvider,
            stateManager,
            checkpointReadQueue,
            inferencer,
            threadPool
        );
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

    enum FeatureTestMode {
        FEATURE_NOT_AVAILABLE,
        ILLEGAL_STATE,
        AD_EXCEPTION
    }

    @SuppressWarnings("unchecked")
    public void featureTestTemplate(FeatureTestMode mode) throws IOException {
        if (mode == FeatureTestMode.FEATURE_NOT_AVAILABLE) {
            doAnswer(invocation -> {
                ActionListener<Optional<double[]>> listener = invocation.getArgument(4);
                listener.onResponse(Optional.empty());
                return null;
            })
                .when(featureQuery)
                .getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), eq(AnalysisType.AD), any(ActionListener.class));
        } else if (mode == FeatureTestMode.ILLEGAL_STATE) {
            doThrow(IllegalArgumentException.class)
                .when(featureQuery)
                .getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), eq(AnalysisType.AD), any(ActionListener.class));
        } else if (mode == FeatureTestMode.AD_EXCEPTION) {
            doThrow(TimeSeriesException.class)
                .when(featureQuery)
                .getCurrentFeatures(any(AnomalyDetector.class), anyLong(), anyLong(), eq(AnalysisType.AD), any(ActionListener.class));
        }

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
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
        new ADSingleStreamResultTransportAction(
            transportService,
            new ActionFilters(Collections.emptySet()),
            adCircuitBreakerService,
            mock(ADCacheProvider.class),
            stateManager,
            mock(ADCheckpointReadWorker.class),
            inferencer,
            threadPool
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

        assertException(listener, TimeSeriesException.class, errLogMsg);
    }

    private void globalBlockTemplate(BlockType type, String errLogMsg) {
        globalBlockTemplate(type, errLogMsg, null, null);
    }

    public void testReadBlock() {
        globalBlockTemplate(BlockType.GLOBAL_BLOCK_READ, ResultProcessor.READ_WRITE_BLOCKED);
    }

    public void testWriteBlock() {
        globalBlockTemplate(BlockType.GLOBAL_BLOCK_WRITE, ResultProcessor.READ_WRITE_BLOCKED);
    }

    public void testIndexReadBlock() {
        globalBlockTemplate(
            BlockType.INDEX_BLOCK,
            ResultProcessor.INDEX_READ_BLOCKED,
            Settings.builder().put(IndexMetadata.INDEX_BLOCKS_READ_SETTING.getKey(), true).build(),
            "test1"
        );
    }

    @SuppressWarnings("unchecked")
    public void testAllFeaturesDisabled() throws IOException {
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(2);
            listener.onFailure(new EndRunException(adID, CommonMessages.ALL_FEATURES_DISABLED_ERR_MSG, true));
            return null;
        }).when(stateManager).getConfig(any(String.class), eq(AnalysisType.AD), any(ActionListener.class));

        AnomalyResultTransportAction action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            clientUtil,
            stateManager,
            featureQuery,
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

        assertException(listener, EndRunException.class, CommonMessages.ALL_FEATURES_DISABLED_ERR_MSG);
    }

    @SuppressWarnings({ "unchecked" })
    public void testColdStartEndRunException() {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(mockThreadPool, new ColdStartConfig.Builder().coldStartRunning(false).build());

        when(stateManager.fetchExceptionAndClear(anyString()))
            .thenReturn(
                Optional
                    .of(
                        new EndRunException(
                            adID,
                            CommonMessages.INVALID_SEARCH_QUERY_MSG,
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
        assertException(listener, EndRunException.class, CommonMessages.INVALID_SEARCH_QUERY_MSG);
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
                            CommonMessages.INVALID_SEARCH_QUERY_MSG,
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
        assertException(listener, EndRunException.class, CommonMessages.INVALID_SEARCH_QUERY_MSG);
        verify(featureQuery, never()).getColdStartData(any(AnomalyDetector.class), any(ActionListener.class));
    }

    public void testColdStartBecauseFailtoGetCheckpoint() throws InterruptedException {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        setUpColdStart(
            mockThreadPool,
            new ColdStartConfig.Builder().getCheckpointException(new IndexNotFoundException(ADCommonName.CHECKPOINT_INDEX_NAME)).build()
        );
        CountDownLatch inProgress = new CountDownLatch(1);
        doAnswer(invocation -> {
            inProgress.countDown();
            return null;
        }).when(checkpointReadQueue).put(any());

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

        inProgress.await(30, TimeUnit.SECONDS);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.001);
        verify(checkpointReadQueue, times(1)).put(any());
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
