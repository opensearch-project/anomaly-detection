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
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Locale;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.common.exception.ADValidationException;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.ValidationAspect;
import org.opensearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler;
import org.opensearch.ad.rest.handler.IndexAnomalyDetectorActionHandler;
import org.opensearch.ad.rest.handler.ValidateAnomalyDetectorActionHandler;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.ValidateAnomalyDetectorResponse;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.rest.RestRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableList;

public class ValidateAnomalyDetectorActionHandlerTests extends AbstractADTest {

    protected AbstractAnomalyDetectorActionHandler<ValidateAnomalyDetectorResponse> handler;
    protected ClusterService clusterService;
    protected ActionListener<ValidateAnomalyDetectorResponse> channel;
    protected TransportService transportService;
    protected AnomalyDetectionIndices anomalyDetectionIndices;
    protected String detectorId;
    protected Long seqNo;
    protected Long primaryTerm;
    protected AnomalyDetector detector;
    protected WriteRequest.RefreshPolicy refreshPolicy;
    protected TimeValue requestTimeout;
    protected Integer maxSingleEntityAnomalyDetectors;
    protected Integer maxMultiEntityAnomalyDetectors;
    protected Integer maxAnomalyFeatures;
    protected Settings settings;
    protected RestRequest.Method method;
    protected ADTaskManager adTaskManager;
    protected SearchFeatureDao searchFeatureDao;
    protected Clock clock;

    @Mock
    private Client clientMock;
    @Mock
    protected ThreadPool threadPool;
    protected ThreadContext threadContext;

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);

        settings = Settings.EMPTY;
        clusterService = mock(ClusterService.class);
        channel = mock(ActionListener.class);
        transportService = mock(TransportService.class);

        anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
        when(anomalyDetectionIndices.doesAnomalyDetectorIndexExist()).thenReturn(true);

        detectorId = "123";
        seqNo = 0L;
        primaryTerm = 0L;
        clock = mock(Clock.class);

        refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE;

        String field = "a";
        detector = TestHelpers
            .randomAnomalyDetectorUsingCategoryFields(detectorId, "timestamp", ImmutableList.of("test-index"), Arrays.asList(field));

        requestTimeout = new TimeValue(1000L);
        maxSingleEntityAnomalyDetectors = 1000;
        maxMultiEntityAnomalyDetectors = 10;
        maxAnomalyFeatures = 5;
        method = RestRequest.Method.POST;
        adTaskManager = mock(ADTaskManager.class);
        searchFeatureDao = mock(SearchFeatureDao.class);

        threadContext = new ThreadContext(settings);
        Mockito.doReturn(threadPool).when(clientMock).threadPool();
        Mockito.doReturn(threadContext).when(threadPool).getThreadContext();
    }

    @SuppressWarnings("unchecked")
    public void testValidateMoreThanThousandSingleEntityDetectorLimit() throws IOException {
        SearchResponse mockResponse = mock(SearchResponse.class);
        int totalHits = maxSingleEntityAnomalyDetectors + 1;
        when(mockResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));
        SearchResponse detectorResponse = mock(SearchResponse.class);
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));
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
                    } else {
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(detector.getIndices().get(0), "timestamp", "date")
                        );
                        listener.onResponse((Response) response);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        NodeClient clientSpy = spy(client);

        handler = new ValidateAnomalyDetectorActionHandler(
            clusterService,
            clientSpy,
            channel,
            anomalyDetectionIndices,
            TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null, true),
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            method,
            xContentRegistry(),
            null,
            searchFeatureDao,
            ValidationAspect.DETECTOR.getName(),
            clock
        );
        handler.start();
        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);
        verify(clientSpy, never()).execute(eq(GetMappingsAction.INSTANCE), any(), any());
        verify(channel).onFailure(response.capture());
        Exception value = response.getValue();
        assertTrue(value instanceof ADValidationException);
        String errorMsg = String
            .format(
                Locale.ROOT,
                IndexAnomalyDetectorActionHandler.EXCEEDED_MAX_SINGLE_ENTITY_DETECTORS_PREFIX_MSG,
                maxSingleEntityAnomalyDetectors
            );
        assertTrue(value.getMessage().contains(errorMsg));
    }

    @SuppressWarnings("unchecked")
    public void testValidateMoreThanTenMultiEntityDetectorsLimit() throws IOException {
        String field = "a";
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        SearchResponse detectorResponse = mock(SearchResponse.class);
        int totalHits = maxMultiEntityAnomalyDetectors + 1;
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));

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
                    } else {
                        GetFieldMappingsResponse response = new GetFieldMappingsResponse(
                            TestHelpers.createFieldMappings(detector.getIndices().get(0), field, "date")
                        );
                        listener.onResponse((Response) response);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        NodeClient clientSpy = spy(client);

        handler = new ValidateAnomalyDetectorActionHandler(
            clusterService,
            clientSpy,
            channel,
            anomalyDetectionIndices,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            method,
            xContentRegistry(),
            null,
            searchFeatureDao,
            "",
            clock
        );
        handler.start();
        ArgumentCaptor<Exception> response = ArgumentCaptor.forClass(Exception.class);
        verify(clientSpy, never()).execute(eq(GetMappingsAction.INSTANCE), any(), any());
        verify(channel).onFailure(response.capture());
        Exception value = response.getValue();
        assertTrue(value instanceof ADValidationException);
        String errorMsg = String
            .format(
                Locale.ROOT,
                IndexAnomalyDetectorActionHandler.EXCEEDED_MAX_MULTI_ENTITY_DETECTORS_PREFIX_MSG,
                maxMultiEntityAnomalyDetectors
            );
        assertTrue(value.getMessage().contains(errorMsg));
    }
}
