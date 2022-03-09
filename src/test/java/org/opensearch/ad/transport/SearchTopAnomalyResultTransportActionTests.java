/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.ADIntegTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.AnomalyResultBucket;
import org.opensearch.ad.transport.handler.ADSearchHandler;
import org.opensearch.client.Client;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableList;

public class SearchTopAnomalyResultTransportActionTests extends ADIntegTestCase {
    private SearchTopAnomalyResultTransportAction action;

    // Helper method to generate the Aggregations obj using the list of result buckets
    private Aggregations generateAggregationsFromBuckets(List<AnomalyResultBucket> buckets, Map<String, Object> mockAfterKeyValue) {
        List<CompositeAggregation.Bucket> bucketList = new ArrayList<>();

        for (AnomalyResultBucket bucket : buckets) {
            InternalMax maxGradeAgg = mock(InternalMax.class);
            when(maxGradeAgg.getName()).thenReturn(AnomalyResultBucket.MAX_ANOMALY_GRADE_FIELD);
            when(maxGradeAgg.getValue()).thenReturn(bucket.getMaxAnomalyGrade());
            CompositeAggregation.Bucket aggBucket = mock(CompositeAggregation.Bucket.class);
            when(aggBucket.getKey()).thenReturn(bucket.getKey());
            when(aggBucket.getDocCount()).thenReturn((long) bucket.getDocCount());
            when(aggBucket.getAggregations()).thenReturn(new Aggregations(new ArrayList<Aggregation>() {
                {
                    add(maxGradeAgg);
                }
            }));
            bucketList.add(aggBucket);
        }

        CompositeAggregation composite = mock(CompositeAggregation.class);
        when(composite.getName()).thenReturn(SearchTopAnomalyResultTransportAction.MULTI_BUCKETS_FIELD);
        when(composite.getBuckets()).thenAnswer((Answer<List<CompositeAggregation.Bucket>>) invocation -> bucketList);
        when(composite.afterKey()).thenReturn(mockAfterKeyValue);

        List<Aggregation> aggList = Collections.singletonList(composite);
        return new Aggregations(aggList);
    }

    // Helper method to generate a SearchResponse obj using the given aggs
    private SearchResponse generateMockSearchResponse(Aggregations aggs) {
        SearchResponseSections sections = new SearchResponseSections(SearchHits.empty(), aggs, null, false, null, null, 1);
        return new SearchResponse(sections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        action = new SearchTopAnomalyResultTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(ADSearchHandler.class),
            mock(Client.class)
        );
    }

    public void testSearchOnNonExistingResultIndex() throws IOException {
        deleteIndexIfExists(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
        String testIndexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        ImmutableList<String> categoryFields = ImmutableList.of("test-field-1", "test-field-2");
        String detectorId = createDetector(
            TestHelpers
                .randomAnomalyDetector(
                    ImmutableList.of(testIndexName),
                    ImmutableList.of(TestHelpers.randomFeature(true)),
                    null,
                    Instant.now(),
                    1,
                    false,
                    categoryFields
                )
        );
        SearchTopAnomalyResultRequest searchRequest = new SearchTopAnomalyResultRequest(
            detectorId,
            null,
            false,
            1,
            Arrays.asList(categoryFields.get(0)),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            Instant.now().minus(10, ChronoUnit.DAYS),
            Instant.now()
        );
        SearchTopAnomalyResultResponse searchResponse = client()
            .execute(SearchTopAnomalyResultAction.INSTANCE, searchRequest)
            .actionGet(10_000);
        assertEquals(searchResponse.getAnomalyResultBuckets().size(), 0);
    }

    @SuppressWarnings("unchecked")
    public void testListenerWithNullResult() {
        ActionListener<SearchTopAnomalyResultResponse> mockListener = mock(ActionListener.class);
        SearchTopAnomalyResultTransportAction.TopAnomalyResultListener listener = action.new TopAnomalyResultListener(
            mockListener, new SearchSourceBuilder(), 1000, 10, SearchTopAnomalyResultTransportAction.OrderType.SEVERITY,
            "custom-result-index-name"
        );
        ArgumentCaptor<Exception> failureCaptor = ArgumentCaptor.forClass(Exception.class);

        listener.onResponse(null);

        verify(mockListener, times(1)).onFailure(failureCaptor.capture());
        assertTrue(failureCaptor.getValue() != null);
    }

    @SuppressWarnings("unchecked")
    public void testListenerWithNullAggregation() {
        ActionListener<SearchTopAnomalyResultResponse> mockListener = mock(ActionListener.class);
        SearchTopAnomalyResultTransportAction.TopAnomalyResultListener listener = action.new TopAnomalyResultListener(
            mockListener, new SearchSourceBuilder(), 1000, 10, SearchTopAnomalyResultTransportAction.OrderType.SEVERITY,
            "custom-result-index-name"
        );

        SearchResponse response = generateMockSearchResponse(null);
        ArgumentCaptor<SearchTopAnomalyResultResponse> responseCaptor = ArgumentCaptor.forClass(SearchTopAnomalyResultResponse.class);

        listener.onResponse(response);

        verify(mockListener, times(1)).onResponse(responseCaptor.capture());
        SearchTopAnomalyResultResponse capturedResponse = responseCaptor.getValue();
        assertTrue(capturedResponse != null);
        assertTrue(capturedResponse.getAnomalyResultBuckets() != null);
        assertEquals(0, capturedResponse.getAnomalyResultBuckets().size());
    }

    @SuppressWarnings("unchecked")
    public void testListenerWithInvalidAggregation() {
        ActionListener<SearchTopAnomalyResultResponse> mockListener = mock(ActionListener.class);
        SearchTopAnomalyResultTransportAction.TopAnomalyResultListener listener = action.new TopAnomalyResultListener(
            mockListener, new SearchSourceBuilder(), 1000, 10, SearchTopAnomalyResultTransportAction.OrderType.SEVERITY,
            "custom-result-index-name"
        );

        // an empty list won't have an entry for 'MULTI_BUCKETS_FIELD' as needed to parse out
        // the expected result buckets, and thus should fail
        Aggregations aggs = new Aggregations(new ArrayList<>());
        SearchResponse response = generateMockSearchResponse(aggs);
        ArgumentCaptor<Exception> failureCaptor = ArgumentCaptor.forClass(Exception.class);

        listener.onResponse(response);

        verify(mockListener, times(1)).onFailure(failureCaptor.capture());
        assertTrue(failureCaptor.getValue() != null);
    }

    @SuppressWarnings("unchecked")
    public void testListenerWithValidEmptyAggregation() {
        ActionListener<SearchTopAnomalyResultResponse> mockListener = mock(ActionListener.class);
        SearchTopAnomalyResultTransportAction.TopAnomalyResultListener listener = action.new TopAnomalyResultListener(
            mockListener, new SearchSourceBuilder(), 1000, 10, SearchTopAnomalyResultTransportAction.OrderType.SEVERITY,
            "custom-result-index-name"
        );

        CompositeAggregation composite = mock(CompositeAggregation.class);
        when(composite.getName()).thenReturn(SearchTopAnomalyResultTransportAction.MULTI_BUCKETS_FIELD);
        when(composite.getBuckets()).thenReturn(new ArrayList<>());
        when(composite.afterKey()).thenReturn(null);
        List<Aggregation> aggList = Collections.singletonList(composite);
        Aggregations aggs = new Aggregations(aggList);

        SearchResponse response = generateMockSearchResponse(aggs);
        ArgumentCaptor<SearchTopAnomalyResultResponse> responseCaptor = ArgumentCaptor.forClass(SearchTopAnomalyResultResponse.class);

        listener.onResponse(response);

        verify(mockListener, times(1)).onResponse(responseCaptor.capture());
        SearchTopAnomalyResultResponse capturedResponse = responseCaptor.getValue();
        assertTrue(capturedResponse != null);
        assertTrue(capturedResponse.getAnomalyResultBuckets() != null);
        assertEquals(0, capturedResponse.getAnomalyResultBuckets().size());
    }

    @SuppressWarnings("unchecked")
    public void testListenerTimesOutWithNoResults() {
        ActionListener<SearchTopAnomalyResultResponse> mockListener = mock(ActionListener.class);
        SearchTopAnomalyResultTransportAction.TopAnomalyResultListener listener = action.new TopAnomalyResultListener(
            mockListener, new SearchSourceBuilder(), 1000, // this is guaranteed to be an expired timestamp
            10, SearchTopAnomalyResultTransportAction.OrderType.OCCURRENCE, "custom-result-index-name"
        );

        Aggregations aggs = generateAggregationsFromBuckets(new ArrayList<>(), new HashMap<String, Object>() {
            {
                put("category-field-name-1", "value-2");
            }
        });
        SearchResponse response = generateMockSearchResponse(aggs);
        ArgumentCaptor<Exception> failureCaptor = ArgumentCaptor.forClass(Exception.class);

        listener.onResponse(response);

        verify(mockListener, times(1)).onFailure(failureCaptor.capture());
        assertTrue(failureCaptor.getValue() != null);
    }

    @SuppressWarnings("unchecked")
    public void testListenerTimesOutWithPartialResults() {
        ActionListener<SearchTopAnomalyResultResponse> mockListener = mock(ActionListener.class);
        SearchTopAnomalyResultTransportAction.TopAnomalyResultListener listener = action.new TopAnomalyResultListener(
            mockListener, new SearchSourceBuilder(), 1000, // this is guaranteed to be an expired timestamp
            10, SearchTopAnomalyResultTransportAction.OrderType.OCCURRENCE, "custom-result-index-name"
        );

        AnomalyResultBucket expectedResponseBucket1 = new AnomalyResultBucket(new HashMap<String, Object>() {
            {
                put("category-field-name-1", "value-1");
            }
        }, 5, 0.2);

        Aggregations aggs = generateAggregationsFromBuckets(new ArrayList<AnomalyResultBucket>() {
            {
                add(expectedResponseBucket1);
            }
        }, new HashMap<String, Object>() {
            {
                put("category-field-name-1", "value-2");
            }
        });

        SearchResponse response = generateMockSearchResponse(aggs);
        ArgumentCaptor<SearchTopAnomalyResultResponse> responseCaptor = ArgumentCaptor.forClass(SearchTopAnomalyResultResponse.class);

        listener.onResponse(response);

        verify(mockListener, times(1)).onResponse(responseCaptor.capture());
        SearchTopAnomalyResultResponse capturedResponse = responseCaptor.getValue();
        assertTrue(capturedResponse != null);
        assertTrue(capturedResponse.getAnomalyResultBuckets() != null);
        assertEquals(1, capturedResponse.getAnomalyResultBuckets().size());
        assertEquals(expectedResponseBucket1, capturedResponse.getAnomalyResultBuckets().get(0));
    }

    @SuppressWarnings("unchecked")
    public void testListenerSortingBySeverity() {
        ActionListener<SearchTopAnomalyResultResponse> mockListener = mock(ActionListener.class);
        SearchTopAnomalyResultTransportAction.TopAnomalyResultListener listener = action.new TopAnomalyResultListener(
            mockListener, new SearchSourceBuilder(), 1000, 10, SearchTopAnomalyResultTransportAction.OrderType.SEVERITY,
            "custom-result-index-name"
        );

        AnomalyResultBucket expectedResponseBucket1 = new AnomalyResultBucket(new HashMap<String, Object>() {
            {
                put("category-field-name-1", "value-1");
            }
        }, 5, 0.2);
        AnomalyResultBucket expectedResponseBucket2 = new AnomalyResultBucket(new HashMap<String, Object>() {
            {
                put("category-field-name-1", "value-2");
            }
        }, 5, 0.3);
        AnomalyResultBucket expectedResponseBucket3 = new AnomalyResultBucket(new HashMap<String, Object>() {
            {
                put("category-field-name-1", "value-3");
            }
        }, 5, 0.1);

        Aggregations aggs = generateAggregationsFromBuckets(new ArrayList<AnomalyResultBucket>() {
            {
                add(expectedResponseBucket1);
                add(expectedResponseBucket2);
                add(expectedResponseBucket3);
            }
        }, null);

        SearchResponse response = generateMockSearchResponse(aggs);
        ArgumentCaptor<SearchTopAnomalyResultResponse> responseCaptor = ArgumentCaptor.forClass(SearchTopAnomalyResultResponse.class);

        listener.onResponse(response);

        verify(mockListener, times(1)).onResponse(responseCaptor.capture());
        SearchTopAnomalyResultResponse capturedResponse = responseCaptor.getValue();
        assertTrue(capturedResponse != null);
        assertTrue(capturedResponse.getAnomalyResultBuckets() != null);
        assertEquals(3, capturedResponse.getAnomalyResultBuckets().size());
        assertEquals(expectedResponseBucket2, capturedResponse.getAnomalyResultBuckets().get(0));
        assertEquals(expectedResponseBucket1, capturedResponse.getAnomalyResultBuckets().get(1));
        assertEquals(expectedResponseBucket3, capturedResponse.getAnomalyResultBuckets().get(2));
    }

    @SuppressWarnings("unchecked")
    public void testListenerSortingByOccurrence() {
        ActionListener<SearchTopAnomalyResultResponse> mockListener = mock(ActionListener.class);
        SearchTopAnomalyResultTransportAction.TopAnomalyResultListener listener = action.new TopAnomalyResultListener(
            mockListener, new SearchSourceBuilder(), 1000, 10, SearchTopAnomalyResultTransportAction.OrderType.OCCURRENCE,
            "custom-result-index-name"
        );

        AnomalyResultBucket expectedResponseBucket1 = new AnomalyResultBucket(new HashMap<String, Object>() {
            {
                put("category-field-name-1", "value-1");
            }
        }, 2, 0.5);
        AnomalyResultBucket expectedResponseBucket2 = new AnomalyResultBucket(new HashMap<String, Object>() {
            {
                put("category-field-name-1", "value-2");
            }
        }, 3, 0.5);
        AnomalyResultBucket expectedResponseBucket3 = new AnomalyResultBucket(new HashMap<String, Object>() {
            {
                put("category-field-name-1", "value-3");
            }
        }, 1, 0.5);

        Aggregations aggs = generateAggregationsFromBuckets(new ArrayList<AnomalyResultBucket>() {
            {
                add(expectedResponseBucket1);
                add(expectedResponseBucket2);
                add(expectedResponseBucket3);
            }
        }, null);

        SearchResponse response = generateMockSearchResponse(aggs);
        ArgumentCaptor<SearchTopAnomalyResultResponse> responseCaptor = ArgumentCaptor.forClass(SearchTopAnomalyResultResponse.class);

        listener.onResponse(response);

        verify(mockListener, times(1)).onResponse(responseCaptor.capture());
        SearchTopAnomalyResultResponse capturedResponse = responseCaptor.getValue();
        assertTrue(capturedResponse != null);
        assertTrue(capturedResponse.getAnomalyResultBuckets() != null);
        assertEquals(3, capturedResponse.getAnomalyResultBuckets().size());
        assertEquals(expectedResponseBucket2, capturedResponse.getAnomalyResultBuckets().get(0));
        assertEquals(expectedResponseBucket1, capturedResponse.getAnomalyResultBuckets().get(1));
        assertEquals(expectedResponseBucket3, capturedResponse.getAnomalyResultBuckets().get(2));
    }
}
