/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.indices.rest.handler;

import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.rest.handler.AggregationPrep;

public class HistogramAggregationHelperTests extends OpenSearchTestCase {

    @Mock
    private Config mockConfig;

    @Mock
    private SearchResponse mockSearchResponse;

    @Mock
    private Aggregations mockAggregations;

    private AggregationPrep histogramAggregationHelper;

    @Mock
    private SearchFeatureDao searchFeatureDao;

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        TimeValue requestTimeout = TimeValue.timeValueMinutes(1);
        histogramAggregationHelper = new AggregationPrep(searchFeatureDao, requestTimeout, mockConfig);
    }

    public void testCheckBucketResultErrors_NullAggregations() {
        when(mockSearchResponse.getAggregations()).thenReturn(null);

        ValidationException exception = assertThrows(ValidationException.class, () -> {
            histogramAggregationHelper.validateAndRetrieveHistogramAggregation(mockSearchResponse);
        });

        verify(mockSearchResponse).getAggregations();
        assert(exception.getMessage().contains(CommonMessages.MODEL_VALIDATION_FAILED_UNEXPECTEDLY));
        assert(exception.getType() == ValidationIssueType.AGGREGATION);
        assert(exception.getAspect() == ValidationAspect.MODEL);
    }

    public void testCheckBucketResultErrors_NullAggregationResult() {
        when(mockSearchResponse.getAggregations()).thenReturn(mockAggregations);
        Histogram histogram = mock(Histogram.class);
        when(histogram.getName()).thenReturn("blah");

        List<Histogram.Bucket> histogramBuckets = Arrays.asList();
        when((List<Histogram.Bucket>)histogram.getBuckets()).thenReturn(histogramBuckets);

        Aggregations searchAggs = new Aggregations(Collections.singletonList(histogram));

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.status()).thenReturn(RestStatus.OK);
        when(searchResponse.getScrollId()).thenReturn(randomAlphaOfLength(1000));
        when(searchResponse.getAggregations()).thenReturn(searchAggs);
        when(searchResponse.getTook()).thenReturn(TimeValue.timeValueMillis(randomNonNegativeLong()));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            histogramAggregationHelper.validateAndRetrieveHistogramAggregation(searchResponse);
        });

        assert(exception.getMessage().contains("Failed to find valid aggregation result"));
    }

    public void testConvertKeyToEpochMillis_Double() {
        Double key = 1234567890.0;
        long expected = 1234567890L;

        long result = AggregationPrep.convertKeyToEpochMillis(key);

        assertEquals("The conversion from Double to long epoch millis is incorrect", expected, result);
    }

    public void testConvertKeyToEpochMillis_Long() {
        Long key = 1234567890L;
        long expected = 1234567890L;

        long result = AggregationPrep.convertKeyToEpochMillis(key);

        assertEquals("The conversion from Long to long epoch millis is incorrect", expected, result);
    }
}
