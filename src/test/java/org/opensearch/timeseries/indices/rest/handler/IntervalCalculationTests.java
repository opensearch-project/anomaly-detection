/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.indices.rest.handler;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TestHelpers;
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

public class IntervalCalculationTests extends OpenSearchTestCase {

    private IntervalCalculation intervalCalculation;
    private Clock clock;
    private ActionListener<IntervalTimeConfiguration> mockIntervalListener;
    private AggregationPrep mockAggregationPrep;
    private Client mockClient;
    private SecurityClientUtil mockClientUtil;
    private User user;
    private Map<String, Object> mockTopEntity;
    private IntervalTimeConfiguration mockIntervalConfig;
    private LongBounds mockLongBounds;
    private Config mockConfig;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        mockIntervalListener = mock(ActionListener.class);
        mockAggregationPrep = mock(AggregationPrep.class);
        mockClient = mock(Client.class);
        mockClientUtil = mock(SecurityClientUtil.class);
        user = TestHelpers.randomUser();
        mockTopEntity = mock(Map.class);
        mockIntervalConfig = mock(IntervalTimeConfiguration.class);
        mockLongBounds = mock(LongBounds.class);
        mockConfig = mock(Config.class);

        intervalCalculation = new IntervalCalculation(
            mockConfig,
            mock(TimeValue.class),
            mockClient,
            mockClientUtil,
            user,
            AnalysisType.AD,
            clock,
            mock(SearchFeatureDao.class),
            System.currentTimeMillis(),
            mockTopEntity
        );
    }

    public void testOnResponseExpirationEpochMsPassed() {
        long expirationEpochMs = clock.millis() - 1000; // Expired 1 second ago

        IntervalRecommendationListener listener = intervalCalculation.new IntervalRecommendationListener(
            mockIntervalListener, new SearchSourceBuilder(), mockIntervalConfig, expirationEpochMs, mockLongBounds
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

    /**
     * AggregationPrep.validateAndRetrieveHistogramAggregation throws ValidationException because
     * response.getAggregations() returns null.
     */
    public void testOnFailure() {
        long expirationEpochMs = clock.millis() - 1000; // Expired 1 second ago
        SearchResponse mockResponse = mock(SearchResponse.class);

        when(mockConfig.getHistoryIntervals()).thenReturn(40);

        IntervalRecommendationListener listener = intervalCalculation.new IntervalRecommendationListener(
            mockIntervalListener, new SearchSourceBuilder(), mockIntervalConfig, expirationEpochMs, mockLongBounds
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
