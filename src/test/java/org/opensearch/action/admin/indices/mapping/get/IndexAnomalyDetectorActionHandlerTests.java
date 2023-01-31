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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.common.exception.ADValidationException;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.IndexAnomalyDetectorActionHandler;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.IndexAnomalyDetectorResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.RestRequest;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 *
 * we need to put the test in the same package of GetFieldMappingsResponse
 * (org.opensearch.action.admin.indices.mapping.get) since its constructor is
 * package private
 *
 */
public class IndexAnomalyDetectorActionHandlerTests extends AbstractADTest {
    static ThreadPool threadPool;
    private String TEXT_FIELD_TYPE = "text";
    private IndexAnomalyDetectorActionHandler handler;
    private SDKClusterService clusterService;
    private SDKRestClient clientMock;
    private TransportService transportService;
    private ActionListener<IndexAnomalyDetectorResponse> channel;
    private AnomalyDetectionIndices anomalyDetectionIndices;
    private String detectorId;
    private Long seqNo;
    private Long primaryTerm;
    private AnomalyDetector detector;
    private WriteRequest.RefreshPolicy refreshPolicy;
    private TimeValue requestTimeout;
    private Integer maxSingleEntityAnomalyDetectors;
    private Integer maxMultiEntityAnomalyDetectors;
    private Integer maxAnomalyFeatures;
    private Settings settings;
    private RestRequest.Method method;
    private ADTaskManager adTaskManager;
    private SearchFeatureDao searchFeatureDao;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("IndexAnomalyDetectorJobActionHandlerTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        settings = Settings.EMPTY;
        clusterService = mock(SDKClusterService.class);
        clientMock = mock(SDKRestClient.class);
        transportService = mock(TransportService.class);

        channel = mock(ActionListener.class);

        anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
        when(anomalyDetectionIndices.doesAnomalyDetectorIndexExist()).thenReturn(true);

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

        method = RestRequest.Method.POST;

        adTaskManager = mock(ADTaskManager.class);

        searchFeatureDao = mock(SearchFeatureDao.class);

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientMock,
            transportService,
            channel,
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
            method,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao
        );
    }

    // we support upto 2 category fields now
    public void testThreeCategoricalFields() throws IOException {
        expectThrows(
            ADValidationException.class,
            () -> TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList("a", "b", "c"))
        );
    }

    @SuppressWarnings("unchecked")
    public void testMoreThanTenThousandSingleEntityDetectors() throws IOException {
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
        // NodeClient client = getCustomNodeClient(detectorResponse, userIndexResponse, detector, threadPool);
        // NodeClient clientSpy = spy(client);

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientMock, // clientSpy,
            transportService,
            channel,
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
            method,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao
        );

        handler.start();
        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);
        // FIXME if we wrap execute on the client, re-enable this
        // https://github.com/opensearch-project/opensearch-sdk-java/issues/368
        // verify(clientMock, never()).execute(eq(GetMappingsAction.INSTANCE), any(), any());
        verify(channel).onFailure(response.capture());
        Exception value = response.getValue();
        assertTrue(value instanceof IllegalArgumentException);
        String errorMsg = String
            .format(
                Locale.ROOT,
                IndexAnomalyDetectorActionHandler.EXCEEDED_MAX_SINGLE_ENTITY_DETECTORS_PREFIX_MSG,
                maxSingleEntityAnomalyDetectors
            );
        assertTrue(value.getMessage().contains(errorMsg));
    }

    @SuppressWarnings("unchecked")
    public void testTextField() throws IOException {
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

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientMock, // client,
            transportService,
            channel,
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
            method,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao
        );

        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);

        handler.start();

        verify(channel).onFailure(response.capture());
        Exception value = response.getValue();
        assertTrue(value instanceof Exception);
        assertTrue(value.getMessage().contains(IndexAnomalyDetectorActionHandler.CATEGORICAL_FIELD_TYPE_ERR_MSG));
    }

    @SuppressWarnings("unchecked")
    private void testValidTypeTemplate(String filedTypeName) throws IOException {
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
                        if (searchRequest.indices()[0].equals(ANOMALY_DETECTORS_INDEX)) {
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

        // NodeClient clientSpy = spy(client);

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientMock, // clientSpy,
            transportService,
            channel,
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
            method,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao
        );

        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);

        handler.start();

        // FIXME if we wrap execute on the client, re-enable this
        // https://github.com/opensearch-project/opensearch-sdk-java/issues/368
        // verify(clientSpy, times(2)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        verify(channel).onFailure(response.capture());
        Exception value = response.getValue();
        assertTrue(value instanceof IllegalArgumentException);
        assertTrue(value.getMessage().contains(IndexAnomalyDetectorActionHandler.NO_DOCS_IN_USER_INDEX_MSG));
    }

    public void testIpField() throws IOException {
        testValidTypeTemplate(CommonName.IP_TYPE);
    }

    public void testKeywordField() throws IOException {
        testValidTypeTemplate(CommonName.KEYWORD_TYPE);
    }

    @SuppressWarnings("unchecked")
    private void testUpdateTemplate(String fieldTypeName) throws IOException {
        String field = "a";
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = 9;
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

        GetResponse getDetectorResponse = TestHelpers
            .createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX);

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
                        if (searchRequest.indices()[0].equals(ANOMALY_DETECTORS_INDEX)) {
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

        // NodeClient clientSpy = spy(client);
        ClusterName clusterName = new ClusterName("test");
        ClusterState clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientMock, // clientSpy,
            transportService,
            channel,
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
            RestRequest.Method.PUT,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao
        );

        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);

        handler.start();

        // FIXME if we wrap execute on the client, re-enable this
        // https://github.com/opensearch-project/opensearch-sdk-java/issues/368
        // verify(clientSpy, times(1)).execute(eq(GetFieldMappingsAction.INSTANCE), any(), any());
        verify(channel).onFailure(response.capture());
        Exception value = response.getValue();
        if (fieldTypeName.equals(CommonName.IP_TYPE) || fieldTypeName.equals(CommonName.KEYWORD_TYPE)) {
            assertTrue(value.getMessage().contains(IndexAnomalyDetectorActionHandler.NO_DOCS_IN_USER_INDEX_MSG));
        } else {
            assertTrue(value.getMessage().contains(IndexAnomalyDetectorActionHandler.CATEGORICAL_FIELD_TYPE_ERR_MSG));
        }
    }

    @Ignore
    public void testUpdateIpField() throws IOException {
        testUpdateTemplate(CommonName.IP_TYPE);
    }

    @Ignore
    public void testUpdateKeywordField() throws IOException {
        testUpdateTemplate(CommonName.KEYWORD_TYPE);
    }

    @Ignore
    public void testUpdateTextField() throws IOException {
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
                        if (searchRequest.indices()[0].equals(ANOMALY_DETECTORS_INDEX)) {
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

    @SuppressWarnings("unchecked")
    public void testMoreThanTenMultiEntityDetectors() throws IOException {
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

        handler = new IndexAnomalyDetectorActionHandler(
            clusterService,
            clientMock, // clientSpy,
            transportService,
            channel,
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
            method,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao
        );

        handler.start();
        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);
        verify(clientSpy, times(1)).search(any(SearchRequest.class), any());
        verify(channel).onFailure(response.capture());
        Exception value = response.getValue();
        assertTrue(value instanceof IllegalArgumentException);
        String errorMsg = String
            .format(
                Locale.ROOT,
                IndexAnomalyDetectorActionHandler.EXCEEDED_MAX_MULTI_ENTITY_DETECTORS_PREFIX_MSG,
                maxMultiEntityAnomalyDetectors
            );
        assertTrue(value.getMessage().contains(errorMsg));
    }

    @Ignore
    @SuppressWarnings("unchecked")
    public void testTenMultiEntityDetectorsUpdateSingleEntityAdToMulti() throws IOException {
        int totalHits = 10;
        AnomalyDetector existingDetector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, null);
        GetResponse getDetectorResponse = TestHelpers
            .createGetResponse(existingDetector, existingDetector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX);

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
            transportService,
            channel,
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
            RestRequest.Method.PUT,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao
        );

        handler.start();

        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);
        verify(clientMock, times(1)).search(any(SearchRequest.class), any());
        verify(clientMock, times(1)).get(any(GetRequest.class), any());
        verify(channel).onFailure(response.capture());
        Exception value = response.getValue();
        assertTrue(value instanceof IllegalArgumentException);
        assertTrue(value.getMessage().contains(IndexAnomalyDetectorActionHandler.EXCEEDED_MAX_MULTI_ENTITY_DETECTORS_PREFIX_MSG));
    }

    @Ignore
    @SuppressWarnings("unchecked")
    public void testTenMultiEntityDetectorsUpdateExistingMultiEntityAd() throws IOException {
        int totalHits = 10;
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList("a"));
        GetResponse getDetectorResponse = TestHelpers
            .createGetResponse(detector, detector.getDetectorId(), AnomalyDetector.ANOMALY_DETECTORS_INDEX);

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
            transportService,
            channel,
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
            RestRequest.Method.PUT,
            xContentRegistry(),
            null,
            adTaskManager,
            searchFeatureDao
        );

        handler.start();

        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);
        verify(clientMock, times(0)).search(any(SearchRequest.class), any());
        verify(clientMock, times(1)).get(any(GetRequest.class), any());
        verify(channel).onFailure(response.capture());
        Exception value = response.getValue();
        // make sure execution passes all necessary checks
        assertTrue(value instanceof IllegalStateException);
        assertTrue(value.getMessage().contains("NodeClient has not been initialized"));
    }
}
