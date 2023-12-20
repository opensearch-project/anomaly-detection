/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.opensearch.ad.indices.ADIndexManagement.ALL_AD_RESULTS_INDEX_PATTERN;
import static org.opensearch.ad.model.AnomalyDetector.DETECTOR_TYPE_FIELD;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.HistoricalAnalysisIntegTestCase;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorType;
import org.opensearch.ad.model.DetectorProfileName;
import org.opensearch.ad.transport.ProfileRequest;
import org.opensearch.ad.transport.ProfileResponse;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonName;

import com.google.common.collect.ImmutableList;

// These tests are intended to ensure the underlying transport actions of the client methods
// are being exercised and returning expected results, covering some of the basic use cases.
// The exhaustive set of transport action scenarios are within the respective transport action
// test suites themselves. We do not want to unnecessarily duplicate all of those tests here.
public class AnomalyDetectionNodeClientTests extends HistoricalAnalysisIntegTestCase {

    private String indexName = "test-data";
    private Instant startTime = Instant.now().minus(2, ChronoUnit.DAYS);
    private AnomalyDetectionNodeClient adClient;
    private PlainActionFuture<SearchResponse> searchResponseFuture;
    private PlainActionFuture<ProfileResponse> profileFuture;

    @Before
    public void setup() {
        adClient = new AnomalyDetectionNodeClient(client());
    }

    @Test
    public void testSearchAnomalyDetectors_NoIndices() {
        deleteIndexIfExists(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS);

        SearchResponse searchResponse = adClient.searchAnomalyDetectors(TestHelpers.matchAllRequest()).actionGet(10000);
        assertEquals(0, searchResponse.getInternalResponse().hits().getTotalHits().value);
    }

    @Test
    public void testSearchAnomalyDetectors_Empty() throws IOException {
        deleteIndexIfExists(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS);
        createDetectorIndex();

        SearchResponse searchResponse = adClient.searchAnomalyDetectors(TestHelpers.matchAllRequest()).actionGet(10000);
        assertEquals(0, searchResponse.getInternalResponse().hits().getTotalHits().value);
    }

    @Test
    public void searchAnomalyDetectors_Populated() throws IOException {
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

        SearchResponse searchResponse = adClient.searchAnomalyDetectors(request).actionGet(10000);
        assertEquals(1, searchResponse.getInternalResponse().hits().getTotalHits().value);
        assertEquals(detectorId, searchResponse.getInternalResponse().hits().getAt(0).getId());
    }

    @Test
    public void testSearchAnomalyResults_NoIndices() {
        searchResponseFuture = mock(PlainActionFuture.class);
        SearchRequest request = new SearchRequest().indices(new String[] {});

        adClient.searchAnomalyResults(request, searchResponseFuture);
        verify(searchResponseFuture).onFailure(any(IllegalArgumentException.class));
    }

    @Test
    public void testSearchAnomalyResults_Empty() throws IOException {
        createADResultIndex();
        SearchResponse searchResponse = adClient
            .searchAnomalyResults(TestHelpers.matchAllRequest().indices(ALL_AD_RESULTS_INDEX_PATTERN))
            .actionGet(10000);
        assertEquals(0, searchResponse.getInternalResponse().hits().getTotalHits().value);
    }

    @Test
    public void testSearchAnomalyResults_Populated() throws IOException {
        deleteIndexIfExists(ALL_AD_RESULTS_INDEX_PATTERN);
        createADResultIndex();
        String adResultId = createADResult(TestHelpers.randomAnomalyDetectResult());

        SearchResponse searchResponse = adClient
            .searchAnomalyResults(TestHelpers.matchAllRequest().indices(ALL_AD_RESULTS_INDEX_PATTERN))
            .actionGet(10000);

        assertEquals(1, searchResponse.getInternalResponse().hits().getTotalHits().value);
        assertEquals(adResultId, searchResponse.getInternalResponse().hits().getAt(0).getId());
    }

    @Test
    public void testGetDetectorProfile_NoIndices() throws ExecutionException, InterruptedException {
        deleteIndexIfExists(CommonName.CONFIG_INDEX);
        deleteIndexIfExists(ALL_AD_RESULTS_INDEX_PATTERN);
        deleteIndexIfExists(ADCommonName.DETECTION_STATE_INDEX);

        profileFuture = mock(PlainActionFuture.class);
        ProfileRequest profileRequest = new ProfileRequest("test-id", new HashSet<DetectorProfileName>(), false);
        ProfileResponse response = adClient.getDetectorProfile(profileRequest).actionGet(10000);

        assertEquals(response.getActiveEntities(), 0);
    }

}
