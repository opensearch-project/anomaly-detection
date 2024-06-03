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

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.IndexAnomalyDetectorActionHandler;
import org.opensearch.ad.rest.handler.ValidateAnomalyDetectorActionHandler;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rest.RestRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.transport.ValidateConfigResponse;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableList;

public class ValidateAnomalyDetectorActionHandlerTests extends AbstractTimeSeriesTest {

    protected ValidateAnomalyDetectorActionHandler handler;
    protected ClusterService clusterService;
    protected ActionListener<ValidateConfigResponse> channel;
    protected TransportService transportService;
    protected ADIndexManagement anomalyDetectionIndices;
    protected String detectorId;
    protected Long seqNo;
    protected Long primaryTerm;
    protected AnomalyDetector detector;
    protected WriteRequest.RefreshPolicy refreshPolicy;
    protected TimeValue requestTimeout;
    protected Integer maxSingleEntityAnomalyDetectors;
    protected Integer maxMultiEntityAnomalyDetectors;
    protected Integer maxAnomalyFeatures;
    protected Integer maxCategoricalFields;
    protected Settings settings;
    protected RestRequest.Method method;
    protected TaskManager<ADTaskCacheManager, ADTaskType, ADTask, ADIndex, ADIndexManagement> adTaskManager;
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

        anomalyDetectionIndices = mock(ADIndexManagement.class);
        when(anomalyDetectionIndices.doesConfigIndexExist()).thenReturn(true);

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
        maxCategoricalFields = 10;
        method = RestRequest.Method.POST;
        adTaskManager = mock(ADTaskManager.class);
        searchFeatureDao = mock(SearchFeatureDao.class);

        threadContext = new ThreadContext(settings);
        Mockito.doReturn(threadPool).when(clientMock).threadPool();
        Mockito.doReturn(threadContext).when(threadPool).getThreadContext();
    }

    public void testValidateMoreThanThousandSingleEntityDetectorLimit() throws IOException, InterruptedException {
        SearchResponse mockResponse = mock(SearchResponse.class);
        int totalHits = maxSingleEntityAnomalyDetectors + 1;
        when(mockResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));
        SearchResponse detectorResponse = mock(SearchResponse.class);
        when(detectorResponse.getHits()).thenReturn(TestHelpers.createSearchHits(totalHits));
        SearchResponse userIndexResponse = mock(SearchResponse.class);
        int userIndexHits = 0;
        when(userIndexResponse.getHits()).thenReturn(TestHelpers.createSearchHits(userIndexHits));
        AnomalyDetector singleEntityDetector = TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null, true);

        // extend NodeClient since its execute method is final and mockito does not allow to mock final methods
        // we can also use spy to overstep the final methods
        NodeClient client = IndexAnomalyDetectorActionHandlerTests
            .getCustomNodeClient(detectorResponse, userIndexResponse, singleEntityDetector, threadPool);

        NodeClient clientSpy = spy(client);
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        SecurityClientUtil clientUtil = new SecurityClientUtil(nodeStateManager, settings);

        handler = new ValidateAnomalyDetectorActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            anomalyDetectionIndices,
            singleEntityDetector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            searchFeatureDao,
            ValidationAspect.DETECTOR.getName(),
            clock,
            settings
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue(e instanceof ValidationException);
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
        verify(clientSpy, never()).execute(eq(GetMappingsAction.INSTANCE), any(), any());
    }

    public void testValidateMoreThanTenMultiEntityDetectorsLimit() throws IOException, InterruptedException {
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
        NodeClient client = IndexAnomalyDetectorActionHandlerTests
            .getCustomNodeClient(detectorResponse, userIndexResponse, detector, threadPool);
        NodeClient clientSpy = spy(client);
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        SecurityClientUtil clientUtil = new SecurityClientUtil(nodeStateManager, settings);

        handler = new ValidateAnomalyDetectorActionHandler(
            clusterService,
            clientSpy,
            clientUtil,
            anomalyDetectionIndices,
            detector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            method,
            xContentRegistry(),
            null,
            searchFeatureDao,
            "",
            clock,
            Settings.EMPTY
        );
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        handler.start(ActionListener.wrap(r -> {
            assertTrue("should not reach here", false);
            inProgressLatch.countDown();
        }, e -> {
            assertTrue(e instanceof ValidationException);
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
        verify(clientSpy, never()).execute(eq(GetMappingsAction.INSTANCE), any(), any());
    }
}
