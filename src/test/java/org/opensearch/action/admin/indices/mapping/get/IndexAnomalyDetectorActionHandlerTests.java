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

package org.opensearch.action.admin.indices.mapping.get;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.IndexAnomalyDetectorActionHandler;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

/**
 *
 * we need to put the test in the same package of GetFieldMappingsResponse
 * (org.opensearch.action.admin.indices.mapping.get) since its constructor is
 * package private
 *
 */
public class IndexAnomalyDetectorActionHandlerTests extends AbstractTimeSeriesTest {
    static ThreadPool threadPool;
    private String TEXT_FIELD_TYPE = "text";
    private IndexAnomalyDetectorActionHandler handler;
    private ClusterService clusterService;
    private NodeClient clientMock;
    private SecurityClientUtil clientUtil;
    private TransportService transportService;
    // private ActionListener<IndexAnomalyDetectorResponse> channel;
    private ADIndexManagement anomalyDetectionIndices;
    private String detectorId;
    private Long seqNo;
    private Long primaryTerm;
    private AnomalyDetector detector;
    private WriteRequest.RefreshPolicy refreshPolicy;
    private TimeValue requestTimeout;
    private Integer maxSingleEntityAnomalyDetectors;
    private Integer maxMultiEntityAnomalyDetectors;
    private Integer maxAnomalyFeatures;
    private Integer maxCategoricalFields;
    private Settings settings;
    private RestRequest.Method method;
    private ADTaskManager adTaskManager;
    private SearchFeatureDao searchFeatureDao;
    private ClusterName clusterName;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("IndexAnomalyDetectorJobActionHandlerTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        settings = Settings.EMPTY;

        clusterService = mock(ClusterService.class);
        ClusterName clusterName = new ClusterName("test");
        ClusterState clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        clientMock = spy(new NodeClient(settings, threadPool));
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        clientUtil = new SecurityClientUtil(nodeStateManager, settings);
        transportService = mock(TransportService.class);

        anomalyDetectionIndices = mock(ADIndexManagement.class);
        when(anomalyDetectionIndices.doesConfigIndexExist()).thenReturn(true);

        detectorId = "123";
        seqNo = 0L;
        primaryTerm = 0L;

        WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE;

        String field = "a";
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        requestTimeout = new TimeValue(1000L);

        maxSingleEntityAnomalyDetectors = 1000;

        maxMultiEntityAnomalyDetectors = 10;

        maxAnomalyFeatures = 5;

        maxCategoricalFields = 2;

        method = RestRequest.Method.POST;

        adTaskManager = mock(ADTaskManager.class);

        searchFeatureDao = mock(SearchFeatureDao.class);

        clusterName = mock(ClusterName.class);
        when(clusterService.getClusterName()).thenReturn(clusterName);
        when(clusterName.value()).thenReturn("test");

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientMock,
            clientUtil,
            transportService,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao,
            Settings.EMPTY
        );
    }

    // we support upto 2 category fields now
    public void testThreeCategoricalFields() throws IOException {
        expectThrows(
            ValidationException.class,
            () -> TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList("a", "b", "c"))
        );
    }

    public void testMoreThanTenThousandSingleEntityDetectors() throws IOException, InterruptedException {
        SearchResponse mockResponse = mock(SearchResponse.class);
        int totalHits = 1001;
        when(mockResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));
        SearchResponse detectorResponse = mock(SearchResponse.class);
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));
        SearchResponse userIndexResponse = mock(SearchResponse.class);
        int userIndexHits = 0;
        when(userIndexResponse.getHits()).thenReturn(TestHelpers.createSearchHits(userIndexHits));

        // extend NodeClient since its execute method is final and mockito does not allow to mock final methods
        // we can also use spy to overstep the final methods
        NodeClient client = getCustomNodeClient(detectorResponse, userIndexResponse, detector, threadPool);
        NodeClient clientSpy = spy(client);
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        clientUtil = new SecurityClientUtil(nodeStateManager, settings);

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            transportService,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            // no categorical feature
            TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null, true),
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao,
            Settings.EMPTY
        );

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should throw eror", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue(e instanceof IllegalArgumentException);
            String errorMsg = String
                .format(
                    Locale.ROOT,
                    IndexAnomalyDetectorActionHandler.EXCEEDED_MAX_SINGLE_STREAM_DETECTORS_PREFIX_MSG,
                    maxSingleEntityAnomalyDetectors
                );
            assertTrue(e.getMessage().contains(errorMsg));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
        verify(clientMock, never()).execute(eq(GetMappingsAction.INSTANCE), any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testTextField() throws IOException, InterruptedException {
        String field = "a";
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = 9;
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        // extend NodeClient since its execute method is final and mockito does not allow to mock final methods
        // we can also use spy to overstep the final methods
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                try {
                    if (action.equals(SearchAction.INSTANCE)) {
                        listener.onResponse((Response) detectorResponse);
                    } else {
                        // passes first get field mapping call where timestamp has to be of type date
                        // fails on second call where categorical field is checked to be type keyword or IP
                        // we need to put the test in the same package of GetFieldMappingsResponse since its constructor is package private
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(detector.getIndices().get(0), field, "date")
                        );
                        listener.onResponse((Response) response);
                    }
                } catch (IOException e) {
                    logger.error("Create field mapping threw an exception", e);
                }
            }
        };
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        clientUtil = new SecurityClientUtil(nodeStateManager, Settings.EMPTY);

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            client,
            clientUtil,
            transportService,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao,
            Settings.EMPTY
        );

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should throw eror", false);
            inProgressLatch.countDown();
        }, e -> {
            String error = String.format(Locale.ROOT, CommonMessages.CATEGORICAL_FIELD_TYPE_ERR_MSG, field);
            assertTrue("actual: " + e.getMessage(), e.getMessage().contains(error));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unchecked")
    private void testValidTypeTemplate(String filedTypeName) throws IOException, InterruptedException {
        String field = "a";
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = 9;
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        SearchResponse userIndexResponse = mock(SearchResponse.class);
        int userIndexHits = 0;
        when(userIndexResponse.getHits()).thenReturn(TestHelpers.createSearchHits(userIndexHits));
        AtomicBoolean isPreCategoryMappingQuery = new AtomicBoolean(true);
        // extend NodeClient since its execute method is final and mockito does not allow to mock final methods
        // we can also use spy to overstep the final methods
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                try {
                    if (action.equals(SearchAction.INSTANCE)) {
                        assertTrue(request instanceof SearchRequest);
                        SearchRequest searchRequest = (SearchRequest) request;
                        if (searchRequest.indices()[0].equals(CommonName.CONFIG_INDEX)) {
                            listener.onResponse((Response) detectorResponse);
                        } else {
                            listener.onResponse((Response) userIndexResponse);
                        }
                    } else if (isPreCategoryMappingQuery.get()) {
                        isPreCategoryMappingQuery.set(false);
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(detector.getIndices().get(0), field, "date")
                        );
                        listener.onResponse((Response) response);
                    } else {
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(detector.getIndices().get(0), field, filedTypeName)
                        );
                        listener.onResponse((Response) response);
                    }
                } catch (IOException e) {
                    logger.error("Create field mapping threw an exception", e);
                }
            }
        };

        NodeClient clientSpy = spy(client);
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        clientUtil = new SecurityClientUtil(nodeStateManager, Settings.EMPTY);

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            transportService,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao,
            Settings.EMPTY
        );

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should throw eror", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue(e instanceof IllegalArgumentException);
            String errorMsg = String
                .format(Locale.ROOT, IndexAnomalyDetectorActionHandler.NO_DOCS_IN_USER_INDEX_MSG, "[" + detector.getIndices().get(0) + "]");
            assertTrue("actual: " + e.getMessage(), e.getMessage().contains(errorMsg));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));

        verify(clientSpy, times(2)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
    }

    public void testIpField() throws IOException, InterruptedException {
        testValidTypeTemplate(CommonName.IP_TYPE);
    }

    public void testKeywordField() throws IOException, InterruptedException {
        testValidTypeTemplate(CommonName.KEYWORD_TYPE);
    }

    @SuppressWarnings("unchecked")
    private void testUpdateTemplate(String fieldTypeName) throws IOException, InterruptedException {
        String field = "a";
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = 9;
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        GetResponse getDetectorResponse = TestHelpers.createGetResponse(detector, detector.getId(), CommonName.CONFIG_INDEX);

        SearchResponse userIndexResponse = mock(SearchResponse.class);
        int userIndexHits = 0;
        when(userIndexResponse.getHits()).thenReturn(TestHelpers.createSearchHits(userIndexHits));

        // extend NodeClient since its execute method is final and mockito does not allow to mock final methods
        // we can also use spy to overstep the final methods
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                try {
                    if (action.equals(SearchAction.INSTANCE)) {
                        assertTrue(request instanceof SearchRequest);
                        SearchRequest searchRequest = (SearchRequest) request;
                        if (searchRequest.indices()[0].equals(CommonName.CONFIG_INDEX)) {
                            listener.onResponse((Response) detectorResponse);
                        } else {
                            listener.onResponse((Response) userIndexResponse);
                        }
                    } else if (action.equals(GetAction.INSTANCE)) {
                        assertTrue(request instanceof GetRequest);
                        listener.onResponse((Response) getDetectorResponse);
                    } else {
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(detector.getIndices().get(0), field, fieldTypeName)
                        );
                        listener.onResponse((Response) response);
                    }
                } catch (IOException e) {
                    logger.error("Create field mapping threw an exception", e);
                }
            }
        };

        NodeClient clientSpy = spy(client);
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        clientUtil = new SecurityClientUtil(nodeStateManager, Settings.EMPTY);
        ClusterName clusterName = new ClusterName("test");
        ClusterState clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            transportService,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            RestRequest.Method.PUT,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao,
            Settings.EMPTY
        );

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should throw eror", false);
            inProgressLatch.countDown();
        }, e -> {
            if (fieldTypeName.equals(CommonName.IP_TYPE) || fieldTypeName.equals(CommonName.KEYWORD_TYPE)) {
                assertTrue(e.getMessage().contains(IndexAnomalyDetectorActionHandler.NO_DOCS_IN_USER_INDEX_MSG));
            } else {
                String error = String.format(Locale.ROOT, CommonMessages.CATEGORICAL_FIELD_TYPE_ERR_MSG, field);
                assertTrue("actual: " + e.getMessage(), e.getMessage().contains(error));
            }
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));

        verify(clientSpy, times(1)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
    }

    @Ignore
    public void testUpdateIpField() throws IOException, InterruptedException {
        testUpdateTemplate(CommonName.IP_TYPE);
    }

    @Ignore
    public void testUpdateKeywordField() throws IOException, InterruptedException {
        testUpdateTemplate(CommonName.KEYWORD_TYPE);
    }

    @Ignore
    public void testUpdateTextField() throws IOException, InterruptedException {
        testUpdateTemplate(TEXT_FIELD_TYPE);
    }

    public static NodeClient getCustomNodeClient(
        SearchResponse detectorResponse,
        SearchResponse userIndexResponse,
        AnomalyDetector detector,
        ThreadPool pool
    ) {
        return new NodeClient(Settings.EMPTY, pool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                try {
                    if (action.equals(SearchAction.INSTANCE)) {
                        assertTrue(request instanceof SearchRequest);
                        SearchRequest searchRequest = (SearchRequest) request;
                        if (searchRequest.indices()[0].equals(CommonName.CONFIG_INDEX)) {
                            listener.onResponse((Response) detectorResponse);
                        } else {
                            listener.onResponse((Response) userIndexResponse);
                        }
                    } else {
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(detector.getIndices().get(0), "timestamp", "date")
                        );
                        listener.onResponse((Response) response);
                    }
                } catch (IOException e) {
                    logger.error("Create field mapping threw an exception", e);
                }
            }
        };
    }

    public void testMoreThanTenMultiEntityDetectors() throws IOException, InterruptedException {
        String field = "a";
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));
        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = 11;
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        SearchResponse userIndexResponse = mock(SearchResponse.class);
        int userIndexHits = 0;
        when(userIndexResponse.getHits()).thenReturn(TestHelpers.createSearchHits(userIndexHits));
        // extend NodeClient since its execute method is final and mockito does not allow to mock final methods
        // we can also use spy to overstep the final methods
        NodeClient client = getCustomNodeClient(detectorResponse, userIndexResponse, detector, threadPool);
        NodeClient clientSpy = spy(client);
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        clientUtil = new SecurityClientUtil(nodeStateManager, settings);

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            transportService,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao,
            Settings.EMPTY
        );

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should throw eror", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue(e instanceof IllegalArgumentException);
            String errorMsg = String
                .format(
                    Locale.ROOT,
                    IndexAnomalyDetectorActionHandler.EXCEEDED_MAX_HC_DETECTORS_PREFIX_MSG,
                    maxMultiEntityAnomalyDetectors
                );
            assertTrue(e.getMessage().contains(errorMsg));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));
        verify(clientSpy, times(1)).search(any(SearchRequest.class), any());
    }

    @Ignore
    @SuppressWarnings("unchecked")
    public void testTenMultiEntityDetectorsUpdateSingleEntityAdToMulti() throws IOException, InterruptedException {
        int totalHits = 10;
        AnomalyDetector existingDetector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, null);
        GetResponse getDetectorResponse = TestHelpers
            .createGetResponse(existingDetector, existingDetector.getId(), CommonName.CONFIG_INDEX);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length == 2
            );

            assertTrue(args[0] instanceof SearchRequest);
            assertTrue(args[1] instanceof ActionListener);

            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];

            listener.onResponse(searchResponse);

            return null;
        }).when(clientMock).search(any(SearchRequest.class), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length == 2
            );

            assertTrue(args[0] instanceof GetRequest);
            assertTrue(args[1] instanceof ActionListener);

            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            listener.onResponse(getDetectorResponse);

            return null;
        }).when(clientMock).get(any(GetRequest.class), any());

        ClusterName clusterName = new ClusterName("test");
        ClusterState clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientMock,
            clientUtil,
            transportService,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            RestRequest.Method.PUT,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao,
            Settings.EMPTY
        );

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should throw eror", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains(IndexAnomalyDetectorActionHandler.EXCEEDED_MAX_HC_DETECTORS_PREFIX_MSG));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));

        verify(clientMock, times(1)).search(any(SearchRequest.class), any());
        verify(clientMock, times(1)).get(any(GetRequest.class), any());
    }

    @Ignore
    @SuppressWarnings("unchecked")
    public void testTenMultiEntityDetectorsUpdateExistingMultiEntityAd() throws IOException, InterruptedException {
        int totalHits = 10;
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList("a"));
        GetResponse getDetectorResponse = TestHelpers.createGetResponse(detector, detector.getId(), CommonName.CONFIG_INDEX);

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length == 2
            );

            assertTrue(args[0] instanceof SearchRequest);
            assertTrue(args[1] instanceof ActionListener);

            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];

            listener.onResponse(searchResponse);

            return null;
        }).when(clientMock).search(any(SearchRequest.class), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length == 2
            );

            assertTrue(args[0] instanceof GetRequest);
            assertTrue(args[1] instanceof ActionListener);

            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            listener.onResponse(getDetectorResponse);

            return null;
        }).when(clientMock).get(any(GetRequest.class), any());

        ClusterName clusterName = new ClusterName("test");
        ClusterState clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientMock,
            clientUtil,
            transportService,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            RestRequest.Method.PUT,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao,
            Settings.EMPTY
        );

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should throw eror", false);
            inProgressLatch.countDown();
        }, e -> {
            // make sure execution passes all necessary checks
            assertTrue(e instanceof IllegalStateException);
            assertTrue(e.getMessage().contains("NodeClient has not been initialized"));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(100, TimeUnit.SECONDS));

        verify(clientMock, times(0)).search(any(SearchRequest.class), any());
        verify(clientMock, times(1)).get(any(GetRequest.class), any());
    }

    public void testUpdateDifferentCategoricalField() throws InterruptedException {
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action.equals(GetFieldMappingsAction.INSTANCE)) {
                    try {
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(detector.getIndices().get(0), "timestamp", "date")
                        );
                        listener.onResponse((Response) response);
                    } catch (IOException e) {
                        logger.error("Create field mapping threw an exception", e);
                    }
                } else if (action.equals(GetAction.INSTANCE)) {
                    // Serialize the object
                    AnomalyDetector clone = new AnomalyDetector(
                        detector.getId(),
                        detector.getVersion(),
                        detector.getName(),
                        detector.getDescription(),
                        detector.getTimeField(),
                        detector.getIndices(),
                        detector.getFeatureAttributes(),
                        detector.getFilterQuery(),
                        detector.getInterval(),
                        detector.getWindowDelay(),
                        detector.getShingleSize(),
                        detector.getUiMetadata(),
                        detector.getSchemaVersion(),
                        Instant.now(),
                        detector.getCategoryFields(),
                        detector.getUser(),
                        "opensearch-ad-plugin-result-blah",
                        detector.getImputationOption(),
                        detector.getRecencyEmphasis(),
                        detector.getSeasonIntervals(),
                        detector.getHistoryIntervals(),
                        null,
                        detector.getCustomResultIndexMinSize(),
                        detector.getCustomResultIndexMinAge(),
                        detector.getCustomResultIndexTTL(),
                        false,
                        Instant.now()
                    );
                    try {
                        listener.onResponse((Response) TestHelpers.createGetResponse(clone, clone.getId(), CommonName.CONFIG_INDEX));
                    } catch (IOException e) {
                        LOG.error(e);
                    }
                } else {
                    assertTrue("should not reach here", false);
                }
            }
        };
        NodeClient clientSpy = spy(client);

        method = RestRequest.Method.PUT;

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            transportService,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            refreshPolicy,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            RestRequest.Method.PUT,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao,
            Settings.EMPTY
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue("actual: " + e, e instanceof OpenSearchStatusException);
            OpenSearchStatusException statusException = (OpenSearchStatusException) e;
            assertTrue(statusException.getMessage().contains(CommonMessages.CAN_NOT_CHANGE_CUSTOM_RESULT_INDEX));
            inProgressLatch.countDown();
        }));
        assertTrue(inProgressLatch.await(10, TimeUnit.SECONDS));
        verify(clientSpy, times(1)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        verify(clientSpy, times(1)).execute(eq(GetAction.INSTANCE), any(), any());
    }
}
