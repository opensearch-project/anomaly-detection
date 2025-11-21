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

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MAX_ENTITIES_PER_QUERY;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_PAGE_SIZE;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.stubbing.Answer;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.Version;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponse.Clusters;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ml.ADColdStart;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.ml.ADRealTimeInferencer;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.ratelimit.ADCheckpointReadWorker;
import org.opensearch.ad.ratelimit.ADColdEntityWorker;
import org.opensearch.ad.ratelimit.ADColdStartWorker;
import org.opensearch.ad.ratelimit.ADResultWriteWorker;
import org.opensearch.ad.ratelimit.ADSaveResultStrategy;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.InternalFailure;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.CompositeRetriever;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.TimeSeriesStat;
import org.opensearch.timeseries.stats.suppliers.CounterSupplier;
import org.opensearch.timeseries.transport.ResultProcessor;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import com.google.common.collect.ImmutableList;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

public class MultiEntityResultTests extends AbstractTimeSeriesTest {
    private AnomalyResultTransportAction action;
    private AnomalyResultRequest request;
    private TransportInterceptor entityResultInterceptor;
    private Clock clock;
    private AnomalyDetector detector;
    private NodeStateManager stateManager;
    private static Settings settings;
    private TransportService transportService;
    private Client client;
    private SecurityClientUtil clientUtil;
    private FeatureManager featureQuery;
    private ADModelManager normalModelManager;
    private HashRing hashRing;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameResolver;
    private CircuitBreakerService adCircuitBreakerService;
    private ADStats adStats;
    private ThreadPool mockThreadPool;
    private String detectorId;
    private Instant now;
    private ADCacheProvider provider;
    private ADIndexManagement indexUtil;
    private ADResultWriteWorker resultWriteQueue;
    private ADCheckpointReadWorker checkpointReadQueue;
    private ADColdStartWorker entityColdStartQueue;
    private ADColdEntityWorker coldEntityQueue;
    private String app0 = "app_0";
    private String server1 = "server_1";
    private String server2 = "server_2";
    private String server3 = "server_3";
    private String serviceField = "service";
    private String hostField = "host";
    private Map<String, Object> attrs1, attrs2, attrs3;
    private ADPriorityCache entityCache;
    private ADTaskManager adTaskManager;
    private ADSaveResultStrategy resultSaver;
    private ADRealTimeInferencer inferencer;
    private Entity entity1, entity2, entity3;

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
        String categoryField = "a";
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Collections.singletonList(categoryField));

        stateManager = mock(NodeStateManager.class);
        // make sure parameters are not null, otherwise this mock won't get invoked
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(stateManager).getConfig(anyString(), eq(AnalysisType.AD), any(boolean.class), any(ActionListener.class));

        settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_COOLDOWN_MINUTES.getKey(), TimeValue.timeValueMinutes(5))
            .put(AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ.getKey(), TimeValue.timeValueHours(1))
            .build();

        // make sure end time is larger enough than Clock.systemUTC().millis() to get PageIterator.hasNext() to pass
        long endTime = Clock.systemUTC().millis() + 100_000;
        Instant historyEnd = Instant.ofEpochMilli(endTime);
        request = new AnomalyResultRequest(detectorId, 100, endTime);

        transportService = mock(TransportService.class);

        client = mock(Client.class);
        ThreadContext threadContext = new ThreadContext(settings);
        mockThreadPool = mock(ThreadPool.class);
        setUpADThreadPool(mockThreadPool);
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);
        clientUtil = new SecurityClientUtil(stateManager, settings);

        featureQuery = mock(FeatureManager.class);

        // normalModelManager = mock(ADModelManager.class);
        normalModelManager = new ADModelManager(
            mock(ADCheckpointDao.class),
            clock,
            10,
            10,
            10,
            0.01,
            10,
            Duration.ofHours(1),
            AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ,
            mock(ADColdStart.class),
            mock(FeatureManager.class),
            mock(MemoryTracker.class),
            settings,
            clusterService
        );

        hashRing = mock(HashRing.class);

        Set<Setting<?>> anomalyResultSetting = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        anomalyResultSetting.add(AD_MAX_ENTITIES_PER_QUERY);
        anomalyResultSetting.add(AD_PAGE_SIZE);
        anomalyResultSetting.add(TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE);
        anomalyResultSetting.add(TimeSeriesSettings.BACKOFF_MINUTES);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, anomalyResultSetting);

        DiscoveryNode discoveryNode = new DiscoveryNode(
            "node1",
            OpenSearchTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        clusterService = ClusterServiceUtils.createClusterService(threadPool, discoveryNode, clusterSettings);

        indexNameResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));

        adCircuitBreakerService = mock(CircuitBreakerService.class);
        when(adCircuitBreakerService.isOpen()).thenReturn(false);

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

        action = new AnomalyResultTransportAction(
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
            xContentRegistry(),
            adTaskManager
        );

        provider = mock(ADCacheProvider.class);
        entityCache = mock(ADPriorityCache.class);
        when(provider.get()).thenReturn(entityCache);
        when(entityCache.get(any(), any()))
            .thenReturn(MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).historyEnd(historyEnd).build()));
        when(entityCache.selectUpdateCandidate(any(), any(), any())).thenReturn(Pair.of(new ArrayList<Entity>(), new ArrayList<Entity>()));

        indexUtil = mock(ADIndexManagement.class);
        resultWriteQueue = mock(ADResultWriteWorker.class);
        resultSaver = new ADSaveResultStrategy(1, resultWriteQueue);
        checkpointReadQueue = mock(ADCheckpointReadWorker.class);
        entityColdStartQueue = mock(ADColdStartWorker.class);

        coldEntityQueue = mock(ADColdEntityWorker.class);

        attrs1 = new HashMap<>();
        attrs1.put(serviceField, app0);
        attrs1.put(hostField, server1);

        attrs2 = new HashMap<>();
        attrs2.put(serviceField, app0);
        attrs2.put(hostField, server2);

        attrs3 = new HashMap<>();
        attrs3.put(serviceField, app0);
        attrs3.put(hostField, server3);

        entity1 = Entity.createEntityByReordering(attrs1);
        entity2 = Entity.createEntityByReordering(attrs2);
        entity3 = Entity.createEntityByReordering(attrs3);

        inferencer = new ADRealTimeInferencer(
            normalModelManager,
            adStats,
            mock(ADCheckpointDao.class),
            entityColdStartQueue,
            resultSaver,
            provider,
            threadPool,
            clock,
            mock(SearchFeatureDao.class)
        );
    }

    @Override
    @After
    public final void tearDown() throws Exception {
        tearDownTestNodes();
        super.tearDown();
    }

    public void testColdStartEndRunException() throws InterruptedException {
        when(stateManager.fetchExceptionAndClear(anyString()))
        .thenReturn(
                Optional
                .of(
                        new EndRunException(
                                detectorId,
                                CommonMessages.INVALID_SEARCH_QUERY_MSG,
                                new NoSuchElementException("No value present"),
                                false
                                )
                        )
                );
        SearchResponse emptyResponse = createEmptyResponse();

        CountDownLatch coordinatingNodeinProgress = new CountDownLatch(1);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            coordinatingNodeinProgress.countDown();
            listener.onResponse(emptyResponse);
            return null;
        }).when(client).search(any(), any());

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);
        assertTrue(coordinatingNodeinProgress.await(10000L, TimeUnit.MILLISECONDS));
        assertException(listener, EndRunException.class, CommonMessages.INVALID_SEARCH_QUERY_MSG);
    }

    // a handler that forwards response or exception received from network
    private <T extends TransportResponse> TransportResponseHandler<T> entityResultHandler(TransportResponseHandler<T> handler) {
        return new TransportResponseHandler<T>() {
            @Override
            public T read(StreamInput in) throws IOException {
                return handler.read(in);
            }

            @Override
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

    private void setUpEntityResult(int nodeIndex, NodeStateManager nodeStateManager) {
        // register entity result action
        new EntityADResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            // since we send requests to testNodes[1]
            testNodes[nodeIndex].transportService,
            adCircuitBreakerService,
            provider,
            nodeStateManager,
            indexUtil,
            checkpointReadQueue,
            coldEntityQueue,
            threadPool,
            inferencer
        );

        // when(normalModelManager.getResult(any(), any(), any(), any(), any())).thenReturn(new ThresholdingResult(0, 1, 1));
    }

    private void setUpEntityResult(int nodeIndex) {
        setUpEntityResult(nodeIndex, stateManager);
    }

    @SuppressWarnings("unchecked")
    public void setUpNormlaStateManager() throws IOException {
        AnomalyDetector detector = TestHelpers.AnomalyDetectorBuilder
            .newInstance(1)
            .setDetectionInterval(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES))
            .setCategoryFields(ImmutableList.of(randomAlphaOfLength(5)))
            .build();
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(TestHelpers.createGetResponse(detector, detectorId, ADCommonName.CONFIG_INDEX));
            return null;
        }).when(client).get(any(GetRequest.class), any(ActionListener.class));

        stateManager = new NodeStateManager(
            client,
            xContentRegistry(),
            settings,
            new ClientUtil(client),
            clock,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            clusterService,
            TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
            TimeSeriesSettings.BACKOFF_MINUTES
        );

        clientUtil = new SecurityClientUtil(stateManager, settings);

        action = new AnomalyResultTransportAction(
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
            xContentRegistry(),
            adTaskManager
        );
    }

    /**
     * Test query error causes EndRunException but not end now
     * @throws InterruptedException when the await are interrupted
     * @throws IOException when failing to create anomaly detector
     */
    public void testQueryErrorEndRunNotNow() throws InterruptedException, IOException {
        setUpNormlaStateManager();

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        String allShardsFailedMsg = "all shards failed";
        // make PageIterator.next return failure
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener
                .onFailure(
                    new SearchPhaseExecutionException(
                        "search",
                        allShardsFailedMsg,
                        new ShardSearchFailure[] { new ShardSearchFailure(new IllegalArgumentException("blah")) }
                    )
                );
            inProgressLatch.countDown();
            return null;
        }).when(client).search(any(), any());

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.001);

        assertTrue(inProgressLatch.await(10000L, TimeUnit.MILLISECONDS));

        PlainActionFuture<AnomalyResultResponse> listener2 = new PlainActionFuture<>();
        action.doExecute(null, request, listener2);
        Exception e = expectThrows(EndRunException.class, () -> listener2.actionGet(10000L));
        // wrapped INVALID_SEARCH_QUERY_MSG around SearchPhaseExecutionException by convertedQueryFailureException
        assertThat("actual message: " + e.getMessage(), e.getMessage(), containsString(CommonMessages.INVALID_SEARCH_QUERY_MSG));
        assertThat("actual message: " + e.getMessage(), e.getMessage(), containsString(allShardsFailedMsg));
        // not end now
        assertTrue(!((EndRunException) e).isEndNow());
    }

    public void testIndexNotFound() throws InterruptedException, IOException {
        setUpNormlaStateManager();

        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        // make PageIterator.next return failure
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onFailure(new IndexNotFoundException("", ""));
            inProgressLatch.countDown();
            return null;
        }).when(client).search(any(), any());

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.001);

        assertTrue(inProgressLatch.await(10000L, TimeUnit.MILLISECONDS));

        PlainActionFuture<AnomalyResultResponse> listener2 = new PlainActionFuture<>();
        action.doExecute(null, request, listener2);
        Exception e = expectThrows(EndRunException.class, () -> listener2.actionGet(10000L));
        assertThat("actual message: " + e.getMessage(), e.getMessage(), containsString(ResultProcessor.TROUBLE_QUERYING_ERR_MSG));
        assertTrue(!((EndRunException) e).isEndNow());
    }

    public void testEmptyFeatures() throws InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(createEmptyResponse());
            inProgressLatch.countDown();
            return null;
        }).when(client).search(any(), any());

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgressLatch.await(10000L, TimeUnit.MILLISECONDS));

        PlainActionFuture<AnomalyResultResponse> listener2 = new PlainActionFuture<>();
        action.doExecute(null, request, listener2);

        AnomalyResultResponse response2 = listener2.actionGet(10000L);
        assertEquals(Double.NaN, response2.getAnomalyGrade(), 0.01);
    }

    /**
     *
     * @return an empty response
     */
    private SearchResponse createEmptyResponse() {
        CompositeAggregation emptyComposite = mock(CompositeAggregation.class);
        when(emptyComposite.getName()).thenReturn(CompositeRetriever.AGG_NAME_COMP);
        when(emptyComposite.afterKey()).thenReturn(null);
        // empty bucket
        when(emptyComposite.getBuckets()).thenAnswer((Answer<List<CompositeAggregation.Bucket>>) invocation -> {
            return new ArrayList<CompositeAggregation.Bucket>();
        });
        Aggregations emptyAggs = new Aggregations(Collections.singletonList(emptyComposite));
        SearchResponseSections emptySections = new SearchResponseSections(SearchHits.empty(), emptyAggs, null, false, null, null, 1);
        return new SearchResponse(emptySections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, Clusters.EMPTY);
    }

    private void setUpSearchResponse() throws IOException {
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(serviceField, hostField));
        // set up a non-empty response
        CompositeAggregation composite = mock(CompositeAggregation.class);
        when(composite.getName()).thenReturn(CompositeRetriever.AGG_NAME_COMP);
        when(composite.afterKey()).thenReturn(attrs3);

        String featureID = detector.getFeatureAttributes().get(0).getId();
        List<CompositeAggregation.Bucket> compositeBuckets = new ArrayList<>();
        CompositeAggregation.Bucket bucket = mock(CompositeAggregation.Bucket.class);
        when(bucket.getKey()).thenReturn(attrs1);
        List<Aggregation> aggList = new ArrayList<>();
        aggList.add(new InternalMin(featureID, randomDouble(), DocValueFormat.RAW, new HashMap<>()));
        Aggregations aggregations = new Aggregations(aggList);
        when(bucket.getAggregations()).thenReturn(aggregations);
        compositeBuckets.add(bucket);

        bucket = mock(CompositeAggregation.Bucket.class);
        when(bucket.getKey()).thenReturn(attrs2);
        aggList = new ArrayList<>();
        aggList.add(new InternalMin(featureID, randomDouble(), DocValueFormat.RAW, new HashMap<>()));
        aggregations = new Aggregations(aggList);
        when(bucket.getAggregations()).thenReturn(aggregations);
        compositeBuckets.add(bucket);

        bucket = mock(CompositeAggregation.Bucket.class);
        when(bucket.getKey()).thenReturn(attrs3);
        aggList = new ArrayList<>();
        aggList.add(new InternalMin(featureID, randomDouble(), DocValueFormat.RAW, new HashMap<>()));
        aggregations = new Aggregations(aggList);
        when(bucket.getAggregations()).thenReturn(aggregations);
        compositeBuckets.add(bucket);

        when(composite.getBuckets()).thenAnswer((Answer<List<CompositeAggregation.Bucket>>) invocation -> { return compositeBuckets; });
        Aggregations aggs = new Aggregations(Collections.singletonList(composite));

        SearchResponseSections sections = new SearchResponseSections(SearchHits.empty(), aggs, null, false, null, null, 1);
        SearchResponse response = new SearchResponse(sections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, Clusters.EMPTY);

        AtomicBoolean firstCalled = new AtomicBoolean();
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            if (firstCalled.get()) {
                listener.onResponse(createEmptyResponse());
            } else {
                // set firstCalled to be true before returning in case that listener return
                // and the 2nd call comes in before firstCalled is set to true. Then we
                // have the 2nd response.
                firstCalled.set(true);
                listener.onResponse(response);
            }
            return null;
        }).when(client).search(any(), any());
    }

    private <T extends TransportResponse> void setUpTransportInterceptor(
        Function<TransportResponseHandler<T>, TransportResponseHandler<T>> interceptor,
        NodeStateManager nodeStateManager
    ) {
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
                        if (action.equals(EntityADResultAction.NAME)) {
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

        // we start support multi-category fields since 1.1
        // Set version to 1.1 will force the outbound/inbound message to use 1.1 version
        setupTestNodes(entityResultInterceptor, 5, settings, Version.V_2_0_0, AD_MAX_ENTITIES_PER_QUERY, AD_PAGE_SIZE);

        TransportService realTransportService = testNodes[0].transportService;
        ClusterService realClusterService = testNodes[0].clusterService;

        action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            realTransportService,
            settings,
            client,
            clientUtil,
            nodeStateManager,
            featureQuery,
            hashRing,
            realClusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            threadPool,
            xContentRegistry(),
            adTaskManager
        );
    }

    private <T extends TransportResponse> void setUpTransportInterceptor(
        Function<TransportResponseHandler<T>, TransportResponseHandler<T>> interceptor
    ) {
        setUpTransportInterceptor(interceptor, stateManager);
    }

    public void testNonEmptyFeatures() throws InterruptedException, IOException {
        setUpSearchResponse();
        setUpTransportInterceptor(this::entityResultHandler);
        // mock hashing ring response. This has to happen after setting up test nodes with the failure interceptor
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(any(String.class)))
            .thenReturn(Optional.of(testNodes[1].discoveryNode()));
        setUpEntityResult(1);

        CountDownLatch modelNodeInProgress = new CountDownLatch(1);
        doAnswer(invocation -> {
            if (modelNodeInProgress.getCount() == 1) {
                modelNodeInProgress.countDown();
            }
            return null;
        }).when(coldEntityQueue).putAll(any());

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(modelNodeInProgress.await(10000L, TimeUnit.MILLISECONDS));

        // since we have 3 results in the first page
        verify(resultWriteQueue, times(3)).putAll(any());
    }

    @SuppressWarnings("unchecked")
    public void testCircuitBreakerOpen() throws InterruptedException, IOException {
        ClientUtil clientUtil = mock(ClientUtil.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(TestHelpers.createGetResponse(detector, detectorId, ADCommonName.CONFIG_INDEX));
            return null;
        }).when(clientUtil).asyncRequest(any(GetRequest.class), any(), any(ActionListener.class));

        stateManager = new NodeStateManager(
            client,
            xContentRegistry(),
            settings,
            clientUtil,
            clock,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            clusterService,
            TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
            TimeSeriesSettings.BACKOFF_MINUTES
        );

        NodeStateManager spyStateManager = spy(stateManager);

        setUpSearchResponse();
        setUpTransportInterceptor(this::entityResultHandler, spyStateManager);
        // mock hashing ring response. This has to happen after setting up test nodes with the failure interceptor
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(any(String.class)))
            .thenReturn(Optional.of(testNodes[1].discoveryNode()));

        CircuitBreakerService openBreaker = mock(CircuitBreakerService.class);
        when(openBreaker.isOpen()).thenReturn(true);

        // register entity result action
        new EntityADResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            // since we send requests to testNodes[1]
            testNodes[1].transportService,
            openBreaker,
            provider,
            spyStateManager,
            indexUtil,
            checkpointReadQueue,
            coldEntityQueue,
            threadPool,
            inferencer
        );

        CountDownLatch inProgress = new CountDownLatch(1);
        doAnswer(invocation -> {
            String id = invocation.getArgument(0);
            Exception exp = invocation.getArgument(1);

            stateManager.setException(id, exp);
            inProgress.countDown();
            return null;
        }).when(spyStateManager).setException(any(), any());

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);
        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));

        listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);
        assertException(listener, LimitExceededException.class, CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG);
    }

    public void testNotAck() throws InterruptedException, IOException {
        setUpSearchResponse();
        setUpTransportInterceptor(this::unackEntityResultHandler);
        // mock hashing ring response. This has to happen after setting up test nodes with the failure interceptor
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(any(String.class)))
            .thenReturn(Optional.of(testNodes[1].discoveryNode()));
        setUpEntityResult(1);

        CountDownLatch inProgress = new CountDownLatch(1);
        doAnswer(invocation -> {
            inProgress.countDown();
            return null;
        }).when(stateManager).addPressure(anyString(), anyString());

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));

        verify(stateManager, times(1)).addPressure(anyString(), anyString());
    }

    public void testMultipleNode() throws InterruptedException, IOException {
        setUpSearchResponse();
        setUpTransportInterceptor(this::entityResultHandler);

        // we use ordered attributes values as the key to hashring
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(eq(entity1.toString())))
            .thenReturn(Optional.of(testNodes[2].discoveryNode()));

        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(eq(entity2.toString())))
            .thenReturn(Optional.of(testNodes[3].discoveryNode()));

        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(eq(entity3.toString())))
            .thenReturn(Optional.of(testNodes[4].discoveryNode()));

        for (int i = 2; i <= 4; i++) {
            setUpEntityResult(i);
        }

        CountDownLatch modelNodeInProgress = new CountDownLatch(3);
        doAnswer(invocation -> {
            modelNodeInProgress.countDown();
            return null;
        }).when(coldEntityQueue).putAll(any());

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(modelNodeInProgress.await(10000L, TimeUnit.MILLISECONDS));

        // since we have 3 results in the first page
        verify(resultWriteQueue, times(3)).putAll(any());
    }

    public void testCacheSelectionError() throws IOException, InterruptedException {
        setUpSearchResponse();
        setUpTransportInterceptor(this::entityResultHandler);
        setUpEntityResult(1);
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(any(String.class)))
            .thenReturn(Optional.of(testNodes[1].discoveryNode()));

        List<Entity> hotEntities = new ArrayList<>();
        Map<String, Object> attrs4 = new HashMap<>();
        attrs4.put(serviceField, app0);
        attrs4.put(hostField, "server_4");
        Entity entity4 = Entity.createEntityByReordering(attrs4);
        hotEntities.add(entity4);

        List<Entity> coldEntities = new ArrayList<>();
        Map<String, Object> attrs5 = new HashMap<>();
        attrs5.put(serviceField, app0);
        attrs5.put(hostField, "server_5");
        Entity entity5 = Entity.createEntityByReordering(attrs5);
        coldEntities.add(entity5);

        when(entityCache.selectUpdateCandidate(any(), any(), any())).thenReturn(Pair.of(hotEntities, coldEntities));

        CountDownLatch modelNodeInProgress = new CountDownLatch(1);
        doAnswer(invocation -> {
            if (modelNodeInProgress.getCount() == 1) {
                modelNodeInProgress.countDown();
            }
            return null;
        }).when(coldEntityQueue).putAll(any());

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        assertTrue(modelNodeInProgress.await(10000L, TimeUnit.MILLISECONDS));
        // size 0 because cacheMissEntities has no record of these entities
        verify(checkpointReadQueue).putAll(argThat(new ArgumentMatcher<List<FeatureRequest>>() {

            @Override
            public boolean matches(List<FeatureRequest> argument) {
                List<FeatureRequest> arg = (argument);
                LOG.info("size: " + arg.size());
                return arg.size() == 0;
            }
        }));

        verify(coldEntityQueue).putAll(argThat(new ArgumentMatcher<List<FeatureRequest>>() {

            @Override
            public boolean matches(List<FeatureRequest> argument) {
                List<FeatureRequest> arg = (argument);
                LOG.info("size: " + arg.size());
                return arg.size() == 0;
            }
        }));
    }

    public void testCacheSelection() throws IOException, InterruptedException {
        setUpSearchResponse();
        setUpTransportInterceptor(this::entityResultHandler);
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(any(String.class)))
            .thenReturn(Optional.of(testNodes[1].discoveryNode()));

        List<Entity> hotEntities = new ArrayList<>();
        Entity entity1 = Entity.createEntityByReordering(attrs1);
        hotEntities.add(entity1);

        List<Entity> coldEntities = new ArrayList<>();
        Entity entity2 = Entity.createEntityByReordering(attrs2);
        coldEntities.add(entity2);

        provider = mock(ADCacheProvider.class);
        entityCache = mock(ADPriorityCache.class);
        when(provider.get()).thenReturn(entityCache);
        when(entityCache.selectUpdateCandidate(any(), any(), any())).thenReturn(Pair.of(hotEntities, coldEntities));
        when(entityCache.get(any(), any())).thenReturn(null);

        new EntityADResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            // since we send requests to testNodes[1]
            testNodes[1].transportService,
            adCircuitBreakerService,
            provider,
            stateManager,
            indexUtil,
            checkpointReadQueue,
            coldEntityQueue,
            threadPool,
            inferencer
        );

        CountDownLatch modelNodeInProgress = new CountDownLatch(1);
        doAnswer(invocation -> {
            if (modelNodeInProgress.getCount() == 1) {
                modelNodeInProgress.countDown();
            }
            return null;
        }).when(coldEntityQueue).putAll(any());

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        assertTrue(modelNodeInProgress.await(10000L, TimeUnit.MILLISECONDS));
        verify(checkpointReadQueue).putAll(argThat(new ArgumentMatcher<List<FeatureRequest>>() {

            @Override
            public boolean matches(List<FeatureRequest> argument) {
                List<FeatureRequest> arg = (argument);
                LOG.info("size: " + arg.size() + " ; element: " + arg.get(0));
                return arg.size() == 1 && arg.get(0).getEntity().get().equals(entity1);
            }
        }));

        verify(coldEntityQueue).putAll(argThat(new ArgumentMatcher<List<FeatureRequest>>() {

            @Override
            public boolean matches(List<FeatureRequest> argument) {
                List<FeatureRequest> arg = (argument);
                LOG.info("size: " + arg.size() + " ; element: " + arg.get(0));
                return arg.size() == 1 && arg.get(0).getEntity().get().equals(entity2);
            }
        }));
    }

    public void testNullFeatures() throws InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        CompositeAggregation emptyComposite = mock(CompositeAggregation.class);
        when(emptyComposite.getName()).thenReturn(null);
        when(emptyComposite.afterKey()).thenReturn(null);
        // empty bucket
        when(emptyComposite.getBuckets()).thenAnswer((Answer<List<CompositeAggregation.Bucket>>) invocation -> {
            return new ArrayList<CompositeAggregation.Bucket>();
        });
        Aggregations emptyAggs = new Aggregations(Collections.singletonList(emptyComposite));
        SearchResponseSections emptySections = new SearchResponseSections(SearchHits.empty(), emptyAggs, null, false, null, null, 1);
        SearchResponse nullResponse = new SearchResponse(emptySections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, Clusters.EMPTY);

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(nullResponse);
            inProgressLatch.countDown();
            return null;
        }).when(client).search(any(), any());

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgressLatch.await(10000L, TimeUnit.MILLISECONDS));

        PlainActionFuture<AnomalyResultResponse> listener2 = new PlainActionFuture<>();
        action.doExecute(null, request, listener2);

        AnomalyResultResponse response2 = listener2.actionGet(10000L);
        assertEquals(Double.NaN, response2.getAnomalyGrade(), 0.01);
    }

    // empty page but non-null after key will make the CompositeRetriever.PageIterator retry
    public void testRetry() throws IOException, InterruptedException {
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(serviceField, hostField));

        // set up empty page but non-null after key
        CompositeAggregation emptyNonNullComposite = mock(CompositeAggregation.class);
        when(emptyNonNullComposite.getName()).thenReturn(CompositeRetriever.AGG_NAME_COMP);
        when(emptyNonNullComposite.afterKey()).thenReturn(attrs3);

        List<CompositeAggregation.Bucket> emptyNonNullCompositeBuckets = new ArrayList<>();
        when(emptyNonNullComposite.getBuckets()).thenAnswer((Answer<List<CompositeAggregation.Bucket>>) invocation -> {
            return emptyNonNullCompositeBuckets;
        });

        Aggregations emptyNonNullAggs = new Aggregations(Collections.singletonList(emptyNonNullComposite));

        SearchResponseSections emptyNonNullSections = new SearchResponseSections(
            SearchHits.empty(),
            emptyNonNullAggs,
            null,
            false,
            null,
            null,
            1
        );
        SearchResponse emptyNonNullResponse = new SearchResponse(
            emptyNonNullSections,
            null,
            1,
            1,
            0,
            0,
            ShardSearchFailure.EMPTY_ARRAY,
            Clusters.EMPTY
        );

        // set up a non-empty response
        CompositeAggregation composite = mock(CompositeAggregation.class);
        when(composite.getName()).thenReturn(CompositeRetriever.AGG_NAME_COMP);
        when(composite.afterKey()).thenReturn(attrs1);

        String featureID = detector.getFeatureAttributes().get(0).getId();
        List<CompositeAggregation.Bucket> compositeBuckets = new ArrayList<>();
        CompositeAggregation.Bucket bucket = mock(CompositeAggregation.Bucket.class);
        when(bucket.getKey()).thenReturn(attrs1);
        List<Aggregation> aggList = new ArrayList<>();
        aggList.add(new InternalMin(featureID, randomDouble(), DocValueFormat.RAW, new HashMap<>()));
        Aggregations aggregations = new Aggregations(aggList);
        when(bucket.getAggregations()).thenReturn(aggregations);
        compositeBuckets.add(bucket);

        when(composite.getBuckets()).thenAnswer((Answer<List<CompositeAggregation.Bucket>>) invocation -> { return compositeBuckets; });
        Aggregations aggs = new Aggregations(Collections.singletonList(composite));

        SearchResponseSections sections = new SearchResponseSections(SearchHits.empty(), aggs, null, false, null, null, 1);
        SearchResponse nonEmptyResponse = new SearchResponse(sections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, Clusters.EMPTY);

        // set up an empty response
        SearchResponse emptyResponse = createEmptyResponse();

        CountDownLatch coordinatingNodeinProgress = new CountDownLatch(3);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            if (coordinatingNodeinProgress.getCount() == 3) {
                coordinatingNodeinProgress.countDown();
                listener.onResponse(emptyNonNullResponse);
            } else if (coordinatingNodeinProgress.getCount() == 2) {
                coordinatingNodeinProgress.countDown();
                listener.onResponse(nonEmptyResponse);
            } else {
                coordinatingNodeinProgress.countDown();
                listener.onResponse(emptyResponse);
            }
            return null;
        }).when(client).search(any(), any());

        // only the EntityResultRequest from nonEmptyResponse will reach model node
        CountDownLatch modelNodeInProgress = new CountDownLatch(1);
        doAnswer(invocation -> {
            if (modelNodeInProgress.getCount() == 1) {
                modelNodeInProgress.countDown();
            }
            return null;
        }).when(coldEntityQueue).putAll(any());

        setUpTransportInterceptor(this::entityResultHandler);
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(any(String.class)))
            .thenReturn(Optional.of(testNodes[1].discoveryNode()));
        setUpEntityResult(1);

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        // since coordinating node and model node run in async model (i.e., coordinating node
        // does not need sync response to proceed next page, we have to make sure both
        // coordinating node and model node finishes before checking assertions)
        assertTrue(coordinatingNodeinProgress.await(10000L, TimeUnit.MILLISECONDS));
        assertTrue(modelNodeInProgress.await(10000L, TimeUnit.MILLISECONDS));

        // since we have 3 results in the first page
        verify(resultWriteQueue, times(1)).putAll(any());
    }

    public void testPageToString() {
        CompositeRetriever retriever = new CompositeRetriever(
            0,
            10,
            detector,
            xContentRegistry(),
            client,
            clientUtil,
            100,
            clock,
            settings,
            10000,
            1000,
            indexNameResolver,
            clusterService,
            AnalysisType.AD
        );
        Map<Entity, double[]> results = new HashMap<>();
        Entity entity1 = Entity.createEntityByReordering(attrs1);
        double[] val = new double[1];
        val[0] = 3.0;
        results.put(entity1, val);
        CompositeRetriever.Page page = retriever.new Page(results);
        String repr = page.toString();
        assertTrue("actual:" + repr, repr.contains(app0));
        assertTrue("actual:" + repr, repr.contains(server1));
    }

    public void testEmptyPageToString() {
        CompositeRetriever retriever = new CompositeRetriever(
            0,
            10,
            detector,
            xContentRegistry(),
            client,
            clientUtil,
            100,
            clock,
            settings,
            10000,
            1000,
            indexNameResolver,
            clusterService,
            AnalysisType.AD
        );

        CompositeRetriever.Page page = retriever.new Page(null);
        String repr = page.toString();
        // we have at least class name
        assertTrue("actual:" + repr, repr.contains("Page"));
    }

    @SuppressWarnings("unchecked")
    private NodeStateManager setUpTestExceptionTestingInModelNode() throws IOException {
        setUpSearchResponse();
        setUpTransportInterceptor(this::entityResultHandler);
        // mock hashing ring response. This has to happen after setting up test nodes with the failure interceptor
        when(hashRing.getOwningNodeWithSameLocalVersionForRealtime(any(String.class)))
            .thenReturn(Optional.of(testNodes[1].discoveryNode()));

        NodeStateManager modelNodeStateManager = mock(NodeStateManager.class);
        CountDownLatch modelNodeInProgress = new CountDownLatch(1);
        // make sure parameters are not null, otherwise this mock won't get invoked
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(3);
            listener.onResponse(Optional.of(detector));
            modelNodeInProgress.countDown();
            return null;
        }).when(modelNodeStateManager).getConfig(anyString(), eq(AnalysisType.AD), any(boolean.class), any(ActionListener.class));
        return modelNodeStateManager;
    }

    public void testEndRunNowInModelNode() throws InterruptedException, IOException {
        NodeStateManager modelNodeStateManager = setUpTestExceptionTestingInModelNode();

        CountDownLatch inProgress = new CountDownLatch(1);
        doAnswer(invocation -> {
            inProgress.countDown();
            return Optional
                .of(
                    new EndRunException(
                        detectorId,
                        CommonMessages.INVALID_SEARCH_QUERY_MSG,
                        new NoSuchElementException("No value present"),
                        true
                    )
                );
        }).when(modelNodeStateManager).fetchExceptionAndClear(anyString());

        when(modelNodeStateManager.fetchExceptionAndClear(anyString()))
            .thenReturn(
                Optional
                    .of(
                        new EndRunException(
                            detectorId,
                            CommonMessages.INVALID_SEARCH_QUERY_MSG,
                            new NoSuchElementException("No value present"),
                            true
                        )
                    )
            );

        setUpEntityResult(1, modelNodeStateManager);

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));

        // since it is end run now, we don't expect any of the normal workflow continues
        verify(resultWriteQueue, never()).putAll(any());
    }

    public void testEndRunNowFalseInModelNode() throws InterruptedException, IOException {
        NodeStateManager modelNodeStateManager = setUpTestExceptionTestingInModelNode();

        when(modelNodeStateManager.fetchExceptionAndClear(anyString()))
            .thenReturn(
                Optional
                    .of(
                        new EndRunException(
                            detectorId,
                            CommonMessages.INVALID_SEARCH_QUERY_MSG,
                            new NoSuchElementException("No value present"),
                            false
                        )
                    )
            );

        setUpEntityResult(1, modelNodeStateManager);

        CountDownLatch inProgress = new CountDownLatch(1);
        doAnswer(invocation -> {
            if (inProgress.getCount() == 1) {
                inProgress.countDown();
            }
            return null;
        }).when(stateManager).setException(anyString(), any());

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));

        // since it is end run now = false, the normal workflow continues
        verify(resultWriteQueue, times(3)).putAll(any());
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(stateManager).setException(anyString(), exceptionCaptor.capture());
        EndRunException endRunException = (EndRunException) (exceptionCaptor.getValue());
        assertTrue(!endRunException.isEndNow());
    }

    /**
     * Test that in model node, previously recorded exception is OpenSearchTimeoutException,
     * @throws IOException when failing to set up transport layer
     * @throws InterruptedException when failing to wait for inProgress to finish
     */
    public void testTimeOutExceptionInModelNode() throws IOException, InterruptedException {
        NodeStateManager modelNodeStateManager = setUpTestExceptionTestingInModelNode();

        when(modelNodeStateManager.fetchExceptionAndClear(anyString())).thenReturn(Optional.of(new OpenSearchTimeoutException("blah")));

        CountDownLatch inProgress = new CountDownLatch(1);
        doAnswer(invocation -> {
            inProgress.countDown();
            return null;
        }).when(stateManager).setException(anyString(), any(Exception.class));

        setUpEntityResult(1, modelNodeStateManager);

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));

        // since OpenSearchTimeoutException is not end run exception (now = true), the normal workflow continues
        verify(resultWriteQueue, times(3)).putAll(any());
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(stateManager).setException(anyString(), exceptionCaptor.capture());
        Exception actual = exceptionCaptor.getValue();
        assertTrue("actual exception is " + actual, actual instanceof InternalFailure);
    }

    /**
     * Test that when both previous and current run returns exception, we return more
     * important exception (EndRunException is more important)
     * @throws InterruptedException when failing to wait for inProgress to finish
     * @throws IOException when failing to set up transport layer
     */
    public void testSelectHigherExceptionInModelNode() throws InterruptedException, IOException {
        when(entityCache.get(any(), any())).thenThrow(EndRunException.class);

        NodeStateManager modelNodeStateManager = setUpTestExceptionTestingInModelNode();

        when(modelNodeStateManager.fetchExceptionAndClear(anyString())).thenReturn(Optional.of(new OpenSearchTimeoutException("blah")));

        setUpEntityResult(1, modelNodeStateManager);

        CountDownLatch inProgress = new CountDownLatch(1);
        doAnswer(invocation -> {
            inProgress.countDown();
            return null;
        }).when(stateManager).setException(anyString(), any(Exception.class));

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));

        // since EndRunException is thrown before getting any result, we cannot save anything
        verify(resultWriteQueue, never()).putAll(any());
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(stateManager).setException(anyString(), exceptionCaptor.capture());
        Exception actual = exceptionCaptor.getValue();
        assertTrue("actual exception is " + actual, actual instanceof EndRunException);
        EndRunException endRunException = (EndRunException) (actual);
        assertTrue(!endRunException.isEndNow());
    }

    /**
     * A missing index will cause the search result to contain null aggregation
     * like {"took":0,"timed_out":false,"_shards":{"total":0,"successful":0,"skipped":0,"failed":0},"hits":{"max_score":0.0,"hits":[]}}
     *
     * The test verifies we can handle such situation and won't throw exceptions
     * @throws InterruptedException while waiting for execution gets interruptted
     */
    public void testMissingIndex() throws InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener
                .onResponse(
                    new SearchResponse(
                        new SearchResponseSections(SearchHits.empty(), null, null, false, null, null, 1),
                        null,
                        1,
                        1,
                        0,
                        0,
                        ShardSearchFailure.EMPTY_ARRAY,
                        Clusters.EMPTY
                    )
                );
            inProgressLatch.countDown();
            return null;
        }).when(client).search(any(), any());

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgressLatch.await(10000L, TimeUnit.MILLISECONDS));
        verify(stateManager, times(1)).setException(eq(detectorId), any(EndRunException.class));
    }
}
