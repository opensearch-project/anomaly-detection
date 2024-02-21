/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.indices.AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN;
import static org.opensearch.ad.model.AnomalyDetector.DETECTOR_TYPE_FIELD;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.HistoricalAnalysisIntegTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.AnomalyDetectorType;
import org.opensearch.ad.model.DetectorProfile;
import org.opensearch.ad.model.DetectorState;
import org.opensearch.ad.transport.GetAnomalyDetectorAction;
import org.opensearch.ad.transport.GetAnomalyDetectorRequest;
import org.opensearch.ad.transport.GetAnomalyDetectorResponse;
import org.opensearch.client.Client;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import com.google.common.collect.ImmutableList;

// These tests are intended to ensure the underlying transport actions of the client methods
// are being exercised and returning expected results, covering some of the basic use cases.
// The exhaustive set of transport action scenarios are within the respective transport action
// test suites themselves. We do not want to unnecessarily duplicate all of those tests here.
public class AnomalyDetectionNodeClientTests extends HistoricalAnalysisIntegTestCase {
    private String indexName = "test-data";
    private Instant startTime = Instant.now().minus(2, ChronoUnit.DAYS);
    private Client clientSpy;
    private AnomalyDetectionNodeClient adClient;
    private PlainActionFuture<SearchResponse> searchResponseFuture;

    @Before
    public void setup() {
        clientSpy = spy(client());
        adClient = new AnomalyDetectionNodeClient(clientSpy, mock(NamedWriteableRegistry.class));
    }

    @Test
    public void testSearchAnomalyDetectors_NoIndices() {
        deleteIndexIfExists(AnomalyDetector.ANOMALY_DETECTORS_INDEX);

        SearchResponse searchResponse = adClient.searchAnomalyDetectors(TestHelpers.matchAllRequest()).actionGet(10000);
        assertEquals(0, searchResponse.getInternalResponse().hits().getTotalHits().value);
    }

    @Test
    public void testSearchAnomalyDetectors_Empty() throws IOException {
        deleteIndexIfExists(AnomalyDetector.ANOMALY_DETECTORS_INDEX);
        createDetectorIndex();

        SearchResponse searchResponse = adClient.searchAnomalyDetectors(TestHelpers.matchAllRequest()).actionGet(10000);
        assertEquals(0, searchResponse.getInternalResponse().hits().getTotalHits().value);
    }

    @Test
    public void searchAnomalyDetectors_Populated() throws IOException {
        ingestTestData(indexName, startTime, 1, "test", 10);
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
        deleteIndexIfExists(AnomalyDetector.ANOMALY_DETECTORS_INDEX);
        deleteIndexIfExists(ALL_AD_RESULTS_INDEX_PATTERN);
        deleteIndexIfExists(CommonName.DETECTION_STATE_INDEX);

        GetAnomalyDetectorRequest profileRequest = new GetAnomalyDetectorRequest(
            "foo",
            Versions.MATCH_ANY,
            true,
            false,
            "",
            "",
            false,
            null
        );

        expectThrows(OpenSearchStatusException.class, () -> adClient.getDetectorProfile(profileRequest).actionGet(10000));

        verify(clientSpy, times(1)).execute(any(GetAnomalyDetectorAction.class), any(), any());
    }

    @Test
    public void testGetDetectorProfile_Populated() throws IOException {
        ingestTestData(indexName, startTime, 1, "test", 10);
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

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<GetAnomalyDetectorResponse> listener = (ActionListener<GetAnomalyDetectorResponse>) args[2];

            // Setting up mock profile to test that the state is returned correctly in the client response
            DetectorProfile mockProfile = mock(DetectorProfile.class);
            when(mockProfile.getState()).thenReturn(DetectorState.DISABLED);

            GetAnomalyDetectorResponse response = new GetAnomalyDetectorResponse(
                1234,
                "4567",
                9876,
                2345,
                detector,
                mock(AnomalyDetectorJob.class),
                false,
                mock(ADTask.class),
                mock(ADTask.class),
                false,
                RestStatus.OK,
                mockProfile,
                null,
                false
            );
            listener.onResponse(response);

            return null;
        }).when(clientSpy).execute(any(GetAnomalyDetectorAction.class), any(), any());

        GetAnomalyDetectorRequest profileRequest = new GetAnomalyDetectorRequest(
            detectorId,
            Versions.MATCH_ANY,
            true,
            false,
            "",
            "",
            false,
            null
        );

        GetAnomalyDetectorResponse response = adClient.getDetectorProfile(profileRequest).actionGet(10000);

        assertNotEquals(null, response.getDetector());
        assertNotEquals(null, response.getDetectorProfile());
        assertEquals(null, response.getAdJob());
        assertEquals(detector.getName(), response.getDetector().getName());
        assertEquals(DetectorState.DISABLED, response.getDetectorProfile().getState());
        verify(clientSpy, times(1)).execute(any(GetAnomalyDetectorAction.class), any(), any());
    }

}
