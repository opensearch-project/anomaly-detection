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

import static org.opensearch.ad.model.AnomalyDetector.DETECTOR_TYPE_FIELD;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.HistoricalAnalysisIntegTestCase;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.TestHelpers;

import com.google.common.collect.ImmutableList;

public class SearchAnomalyDetectorActionTests extends HistoricalAnalysisIntegTestCase {

    private String indexName = "test-data";
    private Instant startTime = Instant.now().minus(2, ChronoUnit.DAYS);

    public void testSearchDetectorAction() throws IOException {
        ingestTestData(indexName, startTime, 1, "test", 3000);
        String detectorType = AnomalyDetectorType.SINGLE_ENTITY.name();
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(indexName),
                ImmutableList.of(TestHelpers.randomFeature(true)),
                null,
                Instant.now(),
                1,
                false,
                null
            );
        createDetectorIndex();
        String detectorId = createDetector(detector);

        BoolQueryBuilder query = new BoolQueryBuilder().filter(new TermQueryBuilder(DETECTOR_TYPE_FIELD, detectorType));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(searchSourceBuilder);

        SearchResponse searchResponse = client().execute(SearchAnomalyDetectorAction.INSTANCE, request).actionGet(10000);
        assertEquals(1, searchResponse.getInternalResponse().hits().getTotalHits().value());
        assertEquals(detectorId, searchResponse.getInternalResponse().hits().getAt(0).getId());
    }

    public void testNoIndex() {
        deleteIndexIfExists(ADCommonName.CONFIG_INDEX);

        BoolQueryBuilder query = new BoolQueryBuilder().filter(new MatchAllQueryBuilder());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query);
        SearchRequest request = new SearchRequest().source(searchSourceBuilder);

        SearchResponse searchResponse = client().execute(SearchAnomalyDetectorAction.INSTANCE, request).actionGet(10000);
        assertEquals(0, searchResponse.getInternalResponse().hits().getTotalHits().value());
    }

}
