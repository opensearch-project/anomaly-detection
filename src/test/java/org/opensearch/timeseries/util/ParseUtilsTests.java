/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.constant.CommonName;

public class ParseUtilsTests extends OpenSearchTestCase {

    public void testGetLatestDataTime_withValidMaxValue() {
        Max maxAggregation = mock(Max.class);
        when(maxAggregation.getValue()).thenReturn(1623840000000.0); // A valid timestamp value

        Aggregations aggregations = new Aggregations(Collections.singletonList(maxAggregation));
        when(maxAggregation.getName()).thenReturn(CommonName.AGG_NAME_MAX_TIME);

        SearchResponseSections sections = new SearchResponseSections(
            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
            aggregations,
            null,
            false,
            null,
            null,
            1
        );
        SearchResponse searchResponse = new SearchResponse(
            sections,
            null,
            0,
            0,
            0,
            0L,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        Optional<Long> result = ParseUtils.getLatestDataTime(searchResponse);

        assertTrue(result.isPresent());
        assertEquals("got " + result.get(), 1623840000000L, result.get().longValue());
    }

    public void testGetLatestDataTime_withNullValue() {
        Max maxAggregation = mock(Max.class);
        when(maxAggregation.getValue()).thenReturn(Double.NEGATIVE_INFINITY); // Simulating a missing value scenario

        Aggregations aggregations = new Aggregations(Collections.singletonList(maxAggregation));
        when(maxAggregation.getName()).thenReturn(CommonName.AGG_NAME_MAX_TIME);

        SearchResponseSections sections = new SearchResponseSections(
            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
            aggregations,
            null,
            false,
            null,
            null,
            1
        );
        SearchResponse searchResponse = new SearchResponse(
            sections,
            null,
            0,
            0,
            0,
            0L,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        // Act
        Optional<Long> result = ParseUtils.getLatestDataTime(searchResponse);

        // Assert
        assertTrue(result.isEmpty());
    }

    public void testGetLatestDataTime_withNoAggregations() {
        // Arrange
        SearchResponseSections sections = new SearchResponseSections(
            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
            null, // No aggregations provided
            null,
            false,
            null,
            null,
            1
        );
        SearchResponse searchResponse = new SearchResponse(
            sections,
            null,
            0,
            0,
            0,
            0L,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        // Act
        Optional<Long> result = ParseUtils.getLatestDataTime(searchResponse);

        // Assert
        assertTrue(result.isEmpty());
    }

    public void testGetLatestDataTime_withNegativeMaxValue() {
        // Arrange
        Max maxAggregation = mock(Max.class);
        when(maxAggregation.getValue()).thenReturn(-9223372036854775808.0); // Invalid negative value

        Aggregations aggregations = new Aggregations(Collections.singletonList(maxAggregation));
        when(maxAggregation.getName()).thenReturn(CommonName.AGG_NAME_MAX_TIME);

        SearchResponseSections sections = new SearchResponseSections(
            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0),
            aggregations,
            null,
            false,
            null,
            null,
            1
        );
        SearchResponse searchResponse = new SearchResponse(
            sections,
            null,
            0,
            0,
            0,
            0L,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        // Act
        Optional<Long> result = ParseUtils.getLatestDataTime(searchResponse);

        // Assert
        assertTrue(result.isEmpty());
    }
}
