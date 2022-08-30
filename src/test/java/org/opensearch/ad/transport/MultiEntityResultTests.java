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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BACKOFF_MINUTES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.PAGE_SIZE;

import java.io.IOException;
import java.time.Clock;
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
import org.junit.*;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.stubbing.Answer;
import org.opensearch.BwcTests;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponse.Clusters;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.caching.EntityCache;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.common.exception.InternalFailure;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.feature.CompositeRetriever;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.ratelimit.CheckpointReadWorker;
import org.opensearch.ad.ratelimit.ColdEntityWorker;
import org.opensearch.ad.ratelimit.EntityFeatureRequest;
import org.opensearch.ad.ratelimit.ResultWriteWorker;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStat;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.stats.StatNames;
import org.opensearch.ad.stats.suppliers.CounterSupplier;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.util.Bwc;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
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
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

import com.google.common.collect.ImmutableList;

@Ignore
public class MultiEntityResultTests extends AbstractADTest {
    private AnomalyResultTransportAction action;
    private AnomalyResultRequest request;
    private TransportInterceptor entityResultInterceptor;
    private Clock clock;
    private AnomalyDetector detector;
    private NodeStateManager stateManager;
    private static Settings settings;
    private TransportService transportService;
    private Client client;
    private FeatureManager featureQuery;
    private ModelManager normalModelManager;
    private HashRing hashRing;
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameResolver;
    private ADCircuitBreakerService adCircuitBreakerService;
    private ADStats adStats;
    private ThreadPool mockThreadPool;
    private String detectorId;
    private Instant now;
    private CacheProvider provider;
    private AnomalyDetectionIndices indexUtil;
    private ResultWriteWorker resultWriteQueue;
    private CheckpointReadWorker checkpointReadQueue;
    private ColdEntityWorker coldEntityQueue;
    private String app0 = "app_0";
    private String server1 = "server_1";
    private String server2 = "server_2";
    private String server3 = "server_3";
    private String serviceField = "service";
    private String hostField = "host";
    private Map<String, Object> attrs1, attrs2, attrs3;
    private EntityCache entityCache;
    private ADTaskManager adTaskManager;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyResultTests.class.getSimpleName());
        Bwc.DISABLE_BWC = false;
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
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(stateManager).getAnomalyDetector(anyString(), any(ActionListener.class));

        settings = Settings.builder().put(AnomalyDetectorSettings.COOLDOWN_MINUTES.getKey(), TimeValue.timeValueMinutes(5)).build();

        // make sure end time is larger enough than Clock.systemUTC().millis() to get PageIterator.hasNext() to pass
        request = new AnomalyResultRequest(detectorId, 100, Clock.systemUTC().millis() + 100_000);

        transportService = mock(TransportService.class);

        client = mock(Client.class);

        featureQuery = mock(FeatureManager.class);

        normalModelManager = mock(ModelManager.class);

        hashRing = mock(HashRing.class);

        Set<Setting<?>> anomalyResultSetting = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        anomalyResultSetting.add(MAX_ENTITIES_PER_QUERY);
        anomalyResultSetting.add(PAGE_SIZE);
        anomalyResultSetting.add(MAX_RETRY_FOR_UNRESPONSIVE_NODE);
        anomalyResultSetting.add(BACKOFF_MINUTES);
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

        adCircuitBreakerService = mock(ADCircuitBreakerService.class);
        when(adCircuitBreakerService.isOpen()).thenReturn(false);

        Map<String, ADStat<?>> statsMap = new HashMap<String, ADStat<?>>() {
            {
                put(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
                put(StatNames.AD_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
                put(StatNames.AD_HC_EXECUTE_REQUEST_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
                put(StatNames.AD_HC_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
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
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool,
            xContentRegistry(),
            adTaskManager
        );

        provider = mock(CacheProvider.class);
        entityCache = mock(EntityCache.class);
        when(provider.get()).thenReturn(entityCache);
        when(entityCache.get(any(), any()))
            .thenReturn(MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build()));
        when(entityCache.selectUpdateCandidate(any(), any(), any())).thenReturn(Pair.of(new ArrayList<Entity>(), new ArrayList<Entity>()));

        indexUtil = mock(AnomalyDetectionIndices.class);
        resultWriteQueue = mock(ResultWriteWorker.class);
        checkpointReadQueue = mock(CheckpointReadWorker.class);

        coldEntityQueue = mock(ColdEntityWorker.class);

        attrs1 = new HashMap<>();
        attrs1.put(serviceField, app0);
        attrs1.put(hostField, server1);

        attrs2 = new HashMap<>();
        attrs2.put(serviceField, app0);
        attrs2.put(hostField, server2);

        attrs3 = new HashMap<>();
        attrs3.put(serviceField, app0);
        attrs3.put(hostField, server3);
    }

    @Override
    @After
    public final void tearDown() throws Exception {
        tearDownTestNodes();
        super.tearDown();
    }

    public void testColdStartEndRunException() {
        when(stateManager.fetchExceptionAndClear(anyString()))
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

    // a handler that forwards response or exception received from network
    private <T extends TransportResponse> TransportResponseHandler<T> entityResultHandler(TransportResponseHandler<T> handler) {
        return new TransportResponseHandler<T>() {
            @Override
            public T read(StreamInput in) throws IOException {
                in.setVersion(BwcTests.V_1_1_0);
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
                in.setVersion(BwcTests.V_1_1_0);
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
        new EntityResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            // since we send requests to testNodes[1]
            testNodes[nodeIndex].transportService,
            normalModelManager,
            adCircuitBreakerService,
            provider,
            nodeStateManager,
            indexUtil,
            resultWriteQueue,
            checkpointReadQueue,
            coldEntityQueue,
            threadPool
        );

        when(normalModelManager.getAnomalyResultForEntity(any(), any(), any(), any(), anyInt()))
            .thenReturn(new ThresholdingResult(0, 1, 1));
    }

    private void setUpEntityResult(int nodeIndex) {
        setUpEntityResult(nodeIndex, stateManager);
    }

    @SuppressWarnings("unchecked")
    public void setUpNormlaStateManager() throws IOException {
        ClientUtil clientUtil = mock(ClientUtil.class);

        AnomalyDetector detector = TestHelpers.AnomalyDetectorBuilder
            .newInstance()
            .setDetectionInterval(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES))
            .setCategoryFields(ImmutableList.of(randomAlphaOfLength(5)))
            .build();
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(TestHelpers.createGetResponse(detector, detectorId, AnomalyDetector.ANOMALY_DETECTORS_INDEX));
            return null;
        }).when(clientUtil).asyncRequest(any(GetRequest.class), any(), any(ActionListener.class));

        stateManager = new NodeStateManager(
            client,
            xContentRegistry(),
            settings,
            clientUtil,
            clock,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            clusterService
        );

        action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            stateManager,
            featureQuery,
            normalModelManager,
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
        assertThat("actual message: " + e.getMessage(), e.getMessage(), containsString(CommonErrorMessages.INVALID_SEARCH_QUERY_MSG));
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
        assertThat(
            "actual message: " + e.getMessage(),
            e.getMessage(),
            containsString(AnomalyResultTransportAction.TROUBLE_QUERYING_ERR_MSG)
        );
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
        when(emptyComposite.getBuckets())
            .thenAnswer((Answer<List<CompositeAggregation.Bucket>>) invocation -> { return new ArrayList<CompositeAggregation.Bucket>(); });
        Aggregations emptyAggs = new Aggregations(Collections.singletonList(emptyComposite));
        SearchResponseSections emptySections = new SearchResponseSections(SearchHits.empty(), emptyAggs, null, false, null, null, 1);
        return new SearchResponse(emptySections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, Clusters.EMPTY);
    }

    private CountDownLatch setUpSearchResponse() throws IOException {
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

        CountDownLatch inProgress = new CountDownLatch(2);
        AtomicBoolean firstCalled = new AtomicBoolean();
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            if (firstCalled.get()) {
                listener.onResponse(createEmptyResponse());
                inProgress.countDown();
            } else {
                // set firstCalled to be true before returning in case that listener return
                // and the 2nd call comes in before firstCalled is set to true. Then we
                // have the 2nd response.
                firstCalled.set(true);
                listener.onResponse(response);
                inProgress.countDown();
            }
            return null;
        }).when(client).search(any(), any());

        return inProgress;
    }

    private <T extends TransportResponse> void setUpTransportInterceptor(
        Function<TransportResponseHandler<T>, TransportResponseHandler<T>> interceptor
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

        // we start support multi-category fields since 1.1
        // Set version to 1.1 will force the outbound/inbound message to use 1.1 version
        setupTestNodes(entityResultInterceptor, 5, settings, BwcTests.V_1_1_0, MAX_ENTITIES_PER_QUERY, PAGE_SIZE);

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

    public void testNonEmptyFeatures() throws InterruptedException, IOException {
        CountDownLatch inProgress = setUpSearchResponse();
        setUpTransportInterceptor(this::entityResultHandler);
        // mock hashing ring response. This has to happen after setting up test nodes with the failure interceptor
        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(any(String.class)))
            .thenReturn(Optional.of(testNodes[1].discoveryNode()));
        setUpEntityResult(1);

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));

        // since we have 3 results in the first page
        verify(resultWriteQueue, times(3)).put(any());
    }

    @SuppressWarnings("unchecked")
    public void testCircuitBreakerOpen() throws InterruptedException, IOException {
        ClientUtil clientUtil = mock(ClientUtil.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(TestHelpers.createGetResponse(detector, detectorId, AnomalyDetector.ANOMALY_DETECTORS_INDEX));
            return null;
        }).when(clientUtil).asyncRequest(any(GetRequest.class), any(), any(ActionListener.class));

        stateManager = new NodeStateManager(
            client,
            xContentRegistry(),
            settings,
            clientUtil,
            clock,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            clusterService
        );

        action = new AnomalyResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            transportService,
            settings,
            client,
            stateManager,
            featureQuery,
            normalModelManager,
            hashRing,
            clusterService,
            indexNameResolver,
            adCircuitBreakerService,
            adStats,
            mockThreadPool,
            xContentRegistry(),
            adTaskManager
        );

        CountDownLatch inProgress = setUpSearchResponse();
        setUpTransportInterceptor(this::entityResultHandler);
        // mock hashing ring response. This has to happen after setting up test nodes with the failure interceptor
        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(any(String.class)))
            .thenReturn(Optional.of(testNodes[1].discoveryNode()));

        ADCircuitBreakerService openBreaker = mock(ADCircuitBreakerService.class);
        when(openBreaker.isOpen()).thenReturn(true);
        // register entity result action
        new EntityResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            // since we send requests to testNodes[1]
            testNodes[1].transportService,
            normalModelManager,
            openBreaker,
            provider,
            stateManager,
            indexUtil,
            resultWriteQueue,
            checkpointReadQueue,
            coldEntityQueue,
            threadPool
        );

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);
        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));

        listener = new PlainActionFuture<>();
        action.doExecute(null, request, listener);
        assertException(listener, LimitExceededException.class, CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG);
    }

    public void testNotAck() throws InterruptedException, IOException {
        CountDownLatch inProgress = setUpSearchResponse();
        setUpTransportInterceptor(this::unackEntityResultHandler);
        // mock hashing ring response. This has to happen after setting up test nodes with the failure interceptor
        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(any(String.class)))
            .thenReturn(Optional.of(testNodes[1].discoveryNode()));
        setUpEntityResult(1);

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));

        verify(stateManager, times(1)).addPressure(anyString(), anyString());
    }

    public void testMultipleNode() throws InterruptedException, IOException {
        CountDownLatch inProgress = setUpSearchResponse();
        setUpTransportInterceptor(this::entityResultHandler);

        Entity entity1 = Entity.createEntityByReordering(attrs1);
        Entity entity2 = Entity.createEntityByReordering(attrs2);
        Entity entity3 = Entity.createEntityByReordering(attrs3);

        // we use ordered attributes values as the key to hashring
        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(eq(entity1.toString())))
            .thenReturn(Optional.of(testNodes[2].discoveryNode()));

        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(eq(entity2.toString())))
            .thenReturn(Optional.of(testNodes[3].discoveryNode()));

        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(eq(entity3.toString())))
            .thenReturn(Optional.of(testNodes[4].discoveryNode()));

        for (int i = 2; i <= 4; i++) {
            setUpEntityResult(i);
        }

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));

        // since we have 3 results in the first page
        verify(resultWriteQueue, times(3)).put(any());
    }

    public void testCacheSelectionError() throws IOException, InterruptedException {
        CountDownLatch inProgress = setUpSearchResponse();
        setUpTransportInterceptor(this::entityResultHandler);
        setUpEntityResult(1);
        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(any(String.class)))
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

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));
        // size 0 because cacheMissEntities has no record of these entities
        verify(checkpointReadQueue).putAll(argThat(new ArgumentMatcher<List<EntityFeatureRequest>>() {

            @Override
            public boolean matches(List<EntityFeatureRequest> argument) {
                List<EntityFeatureRequest> arg = (argument);
                LOG.info("size: " + arg.size());
                return arg.size() == 0;
            }
        }));

        verify(coldEntityQueue).putAll(argThat(new ArgumentMatcher<List<EntityFeatureRequest>>() {

            @Override
            public boolean matches(List<EntityFeatureRequest> argument) {
                List<EntityFeatureRequest> arg = (argument);
                LOG.info("size: " + arg.size());
                return arg.size() == 0;
            }
        }));
    }

    public void testCacheSelection() throws IOException, InterruptedException {
        CountDownLatch inProgress = setUpSearchResponse();
        setUpTransportInterceptor(this::entityResultHandler);
        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(any(String.class)))
            .thenReturn(Optional.of(testNodes[1].discoveryNode()));

        List<Entity> hotEntities = new ArrayList<>();
        Entity entity1 = Entity.createEntityByReordering(attrs1);
        hotEntities.add(entity1);

        List<Entity> coldEntities = new ArrayList<>();
        Entity entity2 = Entity.createEntityByReordering(attrs2);
        coldEntities.add(entity2);

        provider = mock(CacheProvider.class);
        entityCache = mock(EntityCache.class);
        when(provider.get()).thenReturn(entityCache);
        when(entityCache.selectUpdateCandidate(any(), any(), any())).thenReturn(Pair.of(hotEntities, coldEntities));
        when(entityCache.get(any(), any())).thenReturn(null);

        new EntityResultTransportAction(
            new ActionFilters(Collections.emptySet()),
            // since we send requests to testNodes[1]
            testNodes[1].transportService,
            normalModelManager,
            adCircuitBreakerService,
            provider,
            stateManager,
            indexUtil,
            resultWriteQueue,
            checkpointReadQueue,
            coldEntityQueue,
            threadPool
        );

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));
        verify(checkpointReadQueue).putAll(argThat(new ArgumentMatcher<List<EntityFeatureRequest>>() {

            @Override
            public boolean matches(List<EntityFeatureRequest> argument) {
                List<EntityFeatureRequest> arg = (argument);
                LOG.info("size: " + arg.size() + " ; element: " + arg.get(0));
                return arg.size() == 1 && arg.get(0).getEntity().equals(entity1);
            }
        }));

        verify(coldEntityQueue).putAll(argThat(new ArgumentMatcher<List<EntityFeatureRequest>>() {

            @Override
            public boolean matches(List<EntityFeatureRequest> argument) {
                List<EntityFeatureRequest> arg = (argument);
                LOG.info("size: " + arg.size() + " ; element: " + arg.get(0));
                return arg.size() == 1 && arg.get(0).getEntity().equals(entity2);
            }
        }));
    }

    public void testNullFeatures() throws InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        CompositeAggregation emptyComposite = mock(CompositeAggregation.class);
        when(emptyComposite.getName()).thenReturn(null);
        when(emptyComposite.afterKey()).thenReturn(null);
        // empty bucket
        when(emptyComposite.getBuckets())
            .thenAnswer((Answer<List<CompositeAggregation.Bucket>>) invocation -> { return new ArrayList<CompositeAggregation.Bucket>(); });
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
        when(emptyNonNullComposite.getBuckets())
            .thenAnswer((Answer<List<CompositeAggregation.Bucket>>) invocation -> { return emptyNonNullCompositeBuckets; });

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

        CountDownLatch inProgress = new CountDownLatch(3);
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            if (inProgress.getCount() == 3) {
                inProgress.countDown();
                listener.onResponse(emptyNonNullResponse);
            } else if (inProgress.getCount() == 2) {
                inProgress.countDown();
                listener.onResponse(nonEmptyResponse);
            } else {
                inProgress.countDown();
                listener.onResponse(emptyResponse);
            }
            return null;
        }).when(client).search(any(), any());

        setUpTransportInterceptor(this::entityResultHandler);
        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(any(String.class)))
            .thenReturn(Optional.of(testNodes[1].discoveryNode()));
        setUpEntityResult(1);

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));

        // since we have 3 results in the first page
        verify(resultWriteQueue, times(1)).put(any());
    }

    public void testPageToString() {
        CompositeRetriever retriever = new CompositeRetriever(
            0,
            10,
            detector,
            xContentRegistry(),
            client,
            100,
            clock,
            settings,
            10000,
            1000,
            indexNameResolver,
            clusterService
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
            100,
            clock,
            settings,
            10000,
            1000,
            indexNameResolver,
            clusterService
        );

        CompositeRetriever.Page page = retriever.new Page(null);
        String repr = page.toString();
        // we have at least class name
        assertTrue("actual:" + repr, repr.contains("Page"));
    }

    @SuppressWarnings("unchecked")
    private Pair<NodeStateManager, CountDownLatch> setUpTestExceptionTestingInModelNode() throws IOException {
        CountDownLatch inProgress = setUpSearchResponse();
        setUpTransportInterceptor(this::entityResultHandler);
        // mock hashing ring response. This has to happen after setting up test nodes with the failure interceptor
        when(hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(any(String.class)))
            .thenReturn(Optional.of(testNodes[1].discoveryNode()));

        NodeStateManager modelNodeStateManager = mock(NodeStateManager.class);
        // make sure parameters are not null, otherwise this mock won't get invoked
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(modelNodeStateManager).getAnomalyDetector(anyString(), any(ActionListener.class));
        return Pair.of(modelNodeStateManager, inProgress);
    }

    public void testEndRunNowInModelNode() throws InterruptedException, IOException {
        Pair<NodeStateManager, CountDownLatch> preparedFixture = setUpTestExceptionTestingInModelNode();
        NodeStateManager modelNodeStateManager = preparedFixture.getLeft();
        CountDownLatch inProgress = preparedFixture.getRight();

        when(modelNodeStateManager.fetchExceptionAndClear(anyString()))
            .thenReturn(
                Optional
                    .of(
                        new EndRunException(
                            detectorId,
                            CommonErrorMessages.INVALID_SEARCH_QUERY_MSG,
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
        verify(resultWriteQueue, never()).put(any());
    }

    public void testEndRunNowFalseInModelNode() throws InterruptedException, IOException {
        Pair<NodeStateManager, CountDownLatch> preparedFixture = setUpTestExceptionTestingInModelNode();
        NodeStateManager modelNodeStateManager = preparedFixture.getLeft();
        CountDownLatch inProgress = preparedFixture.getRight();

        when(modelNodeStateManager.fetchExceptionAndClear(anyString()))
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

        setUpEntityResult(1, modelNodeStateManager);

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));

        // since it is end run now = false, the normal workflow continues
        verify(resultWriteQueue, times(3)).put(any());
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
        Pair<NodeStateManager, CountDownLatch> preparedFixture = setUpTestExceptionTestingInModelNode();
        NodeStateManager modelNodeStateManager = preparedFixture.getLeft();
        CountDownLatch inProgress = preparedFixture.getRight();

        when(modelNodeStateManager.fetchExceptionAndClear(anyString())).thenReturn(Optional.of(new OpenSearchTimeoutException("blah")));

        setUpEntityResult(1, modelNodeStateManager);

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));

        // since OpenSearchTimeoutException is not end run exception (now = true), the normal workflow continues
        verify(resultWriteQueue, times(3)).put(any());
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

        Pair<NodeStateManager, CountDownLatch> preparedFixture = setUpTestExceptionTestingInModelNode();
        NodeStateManager modelNodeStateManager = preparedFixture.getLeft();
        CountDownLatch inProgress = preparedFixture.getRight();

        when(modelNodeStateManager.fetchExceptionAndClear(anyString())).thenReturn(Optional.of(new OpenSearchTimeoutException("blah")));

        setUpEntityResult(1, modelNodeStateManager);

        PlainActionFuture<AnomalyResultResponse> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);

        AnomalyResultResponse response = listener.actionGet(10000L);
        assertEquals(Double.NaN, response.getAnomalyGrade(), 0.01);

        assertTrue(inProgress.await(10000L, TimeUnit.MILLISECONDS));

        // since EndRunException is thrown before getting any result, we cannot save anything
        verify(resultWriteQueue, never()).put(any());
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(stateManager).setException(anyString(), exceptionCaptor.capture());
        EndRunException endRunException = (EndRunException) (exceptionCaptor.getValue());
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
