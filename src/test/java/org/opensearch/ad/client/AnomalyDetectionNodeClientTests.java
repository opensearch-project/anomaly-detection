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
import static org.opensearch.ad.indices.ADIndexManagement.ALL_AD_RESULTS_INDEX_PATTERN;
import static org.opensearch.ad.model.AnomalyDetector.DETECTOR_TYPE_FIELD;
import static org.opensearch.timeseries.constant.CommonMessages.FAIL_TO_FIND_CONFIG_MSG;

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
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorType;
import org.opensearch.ad.model.DetectorProfile;
import org.opensearch.ad.transport.AnomalyDetectorJobAction;
import org.opensearch.ad.transport.GetAnomalyDetectorAction;
import org.opensearch.ad.transport.GetAnomalyDetectorResponse;
import org.opensearch.ad.transport.IndexAnomalyDetectorAction;
import org.opensearch.ad.transport.IndexAnomalyDetectorRequest;
import org.opensearch.ad.transport.IndexAnomalyDetectorResponse;
import org.opensearch.ad.transport.SuggestAnomalyDetectorParamAction;
import org.opensearch.ad.transport.ValidateAnomalyDetectorAction;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.model.ConfigState;
import org.opensearch.timeseries.model.ConfigValidationIssue;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.transport.GetConfigRequest;
import org.opensearch.timeseries.transport.JobRequest;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.transport.SuggestConfigParamRequest;
import org.opensearch.timeseries.transport.SuggestConfigParamResponse;
import org.opensearch.timeseries.transport.ValidateConfigRequest;
import org.opensearch.timeseries.transport.ValidateConfigResponse;
import org.opensearch.transport.client.Client;

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
        deleteIndexIfExists(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS);

        SearchResponse searchResponse = adClient.searchAnomalyDetectors(TestHelpers.matchAllRequest()).actionGet(10000);
        assertEquals(0, searchResponse.getInternalResponse().hits().getTotalHits().value());
    }

    @Test
    public void testSearchAnomalyDetectors_Empty() throws IOException {
        deleteIndexIfExists(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS);
        createDetectorIndex();

        SearchResponse searchResponse = adClient.searchAnomalyDetectors(TestHelpers.matchAllRequest()).actionGet(10000);
        assertEquals(0, searchResponse.getInternalResponse().hits().getTotalHits().value());
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
        assertEquals(1, searchResponse.getInternalResponse().hits().getTotalHits().value());
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
        assertEquals(0, searchResponse.getInternalResponse().hits().getTotalHits().value());
    }

    @Test
    public void testSearchAnomalyResults_Populated() throws IOException {
        deleteIndexIfExists(ALL_AD_RESULTS_INDEX_PATTERN);
        createADResultIndex();
        String adResultId = createADResult(TestHelpers.randomAnomalyDetectResult());

        SearchResponse searchResponse = adClient
            .searchAnomalyResults(TestHelpers.matchAllRequest().indices(ALL_AD_RESULTS_INDEX_PATTERN))
            .actionGet(10000);

        assertEquals(1, searchResponse.getInternalResponse().hits().getTotalHits().value());
        assertEquals(adResultId, searchResponse.getInternalResponse().hits().getAt(0).getId());
    }

    @Test
    public void testGetDetectorProfile_NoIndices() throws ExecutionException, InterruptedException {
        deleteIndexIfExists(ADCommonName.CONFIG_INDEX);
        deleteIndexIfExists(ALL_AD_RESULTS_INDEX_PATTERN);
        deleteIndexIfExists(ADCommonName.DETECTION_STATE_INDEX);

        GetConfigRequest profileRequest = new GetConfigRequest(
            "foo",
            ADIndex.CONFIG.getIndexName(),
            Versions.MATCH_ANY,
            true,
            false,
            "",
            "",
            false,
            null
        );

        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> adClient.getDetectorProfile(profileRequest).actionGet(10000)
        );

        assertTrue(exception.getMessage().contains(FAIL_TO_FIND_CONFIG_MSG));
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
            when(mockProfile.getState()).thenReturn(ConfigState.DISABLED);

            GetAnomalyDetectorResponse response = new GetAnomalyDetectorResponse(
                1234,
                "4567",
                9876,
                2345,
                detector,
                mock(Job.class),
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

        GetConfigRequest profileRequest = new GetConfigRequest(
            detectorId,
            ADIndex.CONFIG.getIndexName(),
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
        assertEquals(ConfigState.DISABLED, response.getDetectorProfile().getState());
        verify(clientSpy, times(1)).execute(any(GetAnomalyDetectorAction.class), any(), any());
    }

    @Test
    public void testValidateAnomalyDetector() throws IOException {
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

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<ValidateConfigResponse> listener = (ActionListener<ValidateConfigResponse>) args[2];
            ValidateConfigResponse response = new ValidateConfigResponse((ConfigValidationIssue) null);
            listener.onResponse(response);
            return null;
        }).when(clientSpy).execute(any(ValidateAnomalyDetectorAction.class), any(), any());

        ValidateConfigRequest validateRequest = new ValidateConfigRequest(
            org.opensearch.timeseries.AnalysisType.AD,
            detector,
            "detector",
            10,
            10,
            5,
            org.opensearch.common.unit.TimeValue.timeValueSeconds(30),
            2
        );

        ValidateConfigResponse response = adClient.validateAnomalyDetector(validateRequest).actionGet(10000);
        assertNotNull(response);
        verify(clientSpy, times(1)).execute(any(ValidateAnomalyDetectorAction.class), any(), any());
    }

    @Test
    public void testSuggestAnomalyDetector() throws IOException {
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

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<SuggestConfigParamResponse> listener = (ActionListener<SuggestConfigParamResponse>) args[2];
            SuggestConfigParamResponse response = new SuggestConfigParamResponse.Builder().build();
            listener.onResponse(response);
            return null;
        }).when(clientSpy).execute(any(SuggestAnomalyDetectorParamAction.class), any(), any());

        SuggestConfigParamRequest suggestRequest = new SuggestConfigParamRequest(
            org.opensearch.timeseries.AnalysisType.AD,
            detector,
            "detection_interval",
            org.opensearch.common.unit.TimeValue.timeValueSeconds(30)
        );

        SuggestConfigParamResponse response = adClient.suggestAnomalyDetector(suggestRequest).actionGet(10000);
        assertNotNull(response);
        verify(clientSpy, times(1)).execute(any(SuggestAnomalyDetectorParamAction.class), any(), any());
    }

    @Test
    public void testCreateAnomalyDetector() throws IOException {
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

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<IndexAnomalyDetectorResponse> listener = (ActionListener<IndexAnomalyDetectorResponse>) args[2];
            IndexAnomalyDetectorResponse response = new IndexAnomalyDetectorResponse(
                "test-detector-id",
                1L,
                1L,
                1L,
                detector,
                RestStatus.CREATED
            );
            listener.onResponse(response);
            return null;
        }).when(clientSpy).execute(any(IndexAnomalyDetectorAction.class), any(), any());

        IndexAnomalyDetectorRequest createRequest = new IndexAnomalyDetectorRequest(
            "test-detector-id",
            1L,
            1L,
            org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE,
            detector,
            org.opensearch.rest.RestRequest.Method.POST,
            org.opensearch.common.unit.TimeValue.timeValueSeconds(30),
            10,
            10,
            5,
            2
        );

        IndexAnomalyDetectorResponse response = adClient.createAnomalyDetector(createRequest).actionGet(10000);
        assertNotNull(response);
        assertEquals("test-detector-id", response.getId());
        verify(clientSpy, times(1)).execute(any(IndexAnomalyDetectorAction.class), any(), any());
    }

    @Test
    public void testStartAnomalyDetector() throws IOException {
        ingestTestData(indexName, startTime, 1, "test", 10);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<JobResponse> listener = (ActionListener<JobResponse>) args[2];
            JobResponse response = new JobResponse("test-detector-id");
            listener.onResponse(response);
            return null;
        }).when(clientSpy).execute(any(AnomalyDetectorJobAction.class), any(), any());

        JobRequest startRequest = new JobRequest(
            "test-detector-id",
            ADIndex.CONFIG.getIndexName(),
            null,
            false,
            "/_plugins/_anomaly_detection/detectors/test-detector-id/_start"
        );

        JobResponse response = adClient.startAnomalyDetector(startRequest).actionGet(10000);
        assertNotNull(response);
        assertEquals("test-detector-id", response.getId());
        verify(clientSpy, times(1)).execute(any(AnomalyDetectorJobAction.class), any(), any());
    }

}
