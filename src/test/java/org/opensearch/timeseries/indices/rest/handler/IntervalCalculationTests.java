/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.indices.rest.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.rest.handler.AggregationPrep;
import org.opensearch.timeseries.rest.handler.IntervalCalculation;
import org.opensearch.timeseries.rest.handler.IntervalCalculation.IntervalRecommendationListener;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.client.Client;

public class IntervalCalculationTests extends OpenSearchTestCase {

    private IntervalCalculation intervalCalculation;
    private Clock clock;
    private ActionListener<IntervalTimeConfiguration> mockIntervalListener;
    private SecurityClientUtil mockClientUtil;
    private User user;
    private Map<String, Object> mockTopEntity;
    private IntervalTimeConfiguration mockIntervalConfig;
    private LongBounds mockLongBounds;
    private Config mockConfig;
    private SearchFeatureDao searchFeatureDao;

    @Mock
    private Client client;

    @Mock
    private ThreadPool threadPool;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);
        clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        mockIntervalListener = mock(ActionListener.class);
        mockClientUtil = new SecurityClientUtil(mock(NodeStateManager.class), Settings.EMPTY);
        user = TestHelpers.randomUser();
        mockTopEntity = mock(Map.class);
        mockIntervalConfig = mock(IntervalTimeConfiguration.class);
        mockLongBounds = mock(LongBounds.class);
        mockConfig = mock(Config.class);
        searchFeatureDao = mock(SearchFeatureDao.class);
        ExecutorService executorService = mock(ExecutorService.class);
        when(threadPool.executor(TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME)).thenReturn(executorService);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executorService).execute(any(Runnable.class));

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);

        intervalCalculation = new IntervalCalculation(
            mockConfig,
            mock(TimeValue.class),
            client,
            mockClientUtil,
            user,
            AnalysisType.AD,
            clock,
            searchFeatureDao,
            System.currentTimeMillis(),
            mockTopEntity,
            false
        );
    }

    public void testOnResponseExpirationEpochMsPassed() {
        long expirationEpochMs = clock.millis() - 1000; // Expired 1 second ago

        IntervalRecommendationListener listener = intervalCalculation.new IntervalRecommendationListener(
            mockIntervalListener, mockIntervalConfig, expirationEpochMs, mockLongBounds
        );

        Histogram histogram = mock(Histogram.class);
        when(histogram.getName()).thenReturn(AggregationPrep.AGGREGATION);
        Aggregations aggs = new Aggregations(Arrays.asList(histogram));
        SearchResponseSections sections = new SearchResponseSections(
            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
            aggs,
            null,
            false,
            null,
            null,
            1
        );
        listener.onResponse(new SearchResponse(sections, null, 0, 0, 0, 0L, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY));

        ArgumentCaptor<ValidationException> argumentCaptor = ArgumentCaptor.forClass(ValidationException.class);
        verify(mockIntervalListener).onFailure(argumentCaptor.capture());
        ValidationException validationException = argumentCaptor.getValue();
        assertEquals(CommonMessages.TIMEOUT_ON_INTERVAL_REC, validationException.getMessage());
        assertEquals(ValidationIssueType.TIMEOUT, validationException.getType());
        assertEquals(ValidationAspect.MODEL, validationException.getAspect());
    }

    public void testRefineGapFallsBackToAutoDate() {
        intervalCalculation = spy(intervalCalculation);
        // Stub the runAutoDate method to do nothing, so we can verify it was called.
        doNothing().when(intervalCalculation).runAutoDate(any(), any(), any(), any());

        // Call refineGap with depth > MAX_SPLIT_DEPTH
        intervalCalculation.refineGap(10, -1, new BoolQueryBuilder(), mockIntervalListener, 1, ChronoUnit.MINUTES, "timestamp", 11, 0L, 1L);

        // Verify that runAutoDate was called
        verify(intervalCalculation, times(1)).runAutoDate(any(), any(), any(), any());
    }

    public void testRunAutoDateReturnsCorrectInterval() throws IOException {
        // Mock the search response for runAutoDate
        SearchResponse mockSearchResponse = mock(SearchResponse.class);
        NumericMetricsAggregation.SingleValue mockShortest = mock(NumericMetricsAggregation.SingleValue.class);
        when(mockShortest.getName()).thenReturn("shortest");
        // gap of 2.5 minutes (150,000 ms)
        when(mockShortest.value()).thenReturn(150000.0);

        // Create a real Aggregations object containing our mock
        Aggregations aggregations = new Aggregations(Arrays.asList(mockShortest));
        when(mockSearchResponse.getAggregations()).thenReturn(aggregations);

        // Mock the client to return this response
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(mockSearchResponse);
            return null;
        }).when(client).search(any(), any());

        // Call runAutoDate
        intervalCalculation.runAutoDate(new BoolQueryBuilder(), mockIntervalListener, ChronoUnit.MINUTES, "timestamp");

        // Capture the result and assert
        ArgumentCaptor<IntervalTimeConfiguration> captor = ArgumentCaptor.forClass(IntervalTimeConfiguration.class);
        verify(mockIntervalListener).onResponse(captor.capture());

        IntervalTimeConfiguration result = captor.getValue();
        // 150000 ms is 2.5 minutes. toCeilMinutes should make it 3.
        assertEquals(3, result.getInterval());
        assertEquals(ChronoUnit.MINUTES, result.getUnit());
    }

    /**
     * AggregationPrep.validateAndRetrieveHistogramAggregation throws ValidationException because
     * response.getAggregations() returns null.
     */
    public void testOnFailure() {
        long expirationEpochMs = clock.millis() - 1000; // Expired 1 second ago
        SearchResponse mockResponse = mock(SearchResponse.class);

        when(mockConfig.getHistoryIntervals()).thenReturn(40);
        doThrow(IllegalArgumentException.class)
            .when(searchFeatureDao)
            .countContinuousShinglesFromHistogramSearch(any(), any(), anyBoolean());

        IntervalRecommendationListener listener = intervalCalculation.new IntervalRecommendationListener(
            mockIntervalListener, mockIntervalConfig, expirationEpochMs, mockLongBounds
        );

        listener.onResponse(mockResponse);

        ArgumentCaptor<ValidationException> argumentCaptor = ArgumentCaptor.forClass(ValidationException.class);
        verify(mockIntervalListener).onFailure(argumentCaptor.capture());
        ValidationException validationException = argumentCaptor.getValue();
        assertEquals(CommonMessages.MODEL_VALIDATION_FAILED_UNEXPECTEDLY, validationException.getMessage());
        assertEquals(ValidationIssueType.AGGREGATION, validationException.getType());
        assertEquals(ValidationAspect.MODEL, validationException.getAspect());
    }
}
