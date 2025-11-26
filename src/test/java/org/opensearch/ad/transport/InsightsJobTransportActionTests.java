/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.Collections;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponse.Clusters;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.rest.handler.InsightsJobActionHandler;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.transport.InsightsJobRequest;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class InsightsJobTransportActionTests extends OpenSearchTestCase {

    private TransportService transportService;
    private Client client;
    private InsightsJobActionHandler jobHandler;
    private InsightsJobTransportAction transportAction;

    @Before
    public void setUpTransportAction() throws Exception {
        transportService = mock(TransportService.class);
        client = mock(Client.class);
        ClusterService clusterService = mock(ClusterService.class);
        ADIndexManagement indexManagement = mock(ADIndexManagement.class);

        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(30))
            .build();

        transportAction = new InsightsJobTransportAction(
            transportService,
            new ActionFilters(Collections.emptySet()),
            client,
            clusterService,
            settings,
            NamedXContentRegistry.EMPTY,
            indexManagement
        );

        jobHandler = mock(InsightsJobActionHandler.class);
        Field handlerField = InsightsJobTransportAction.class.getDeclaredField("jobHandler");
        handlerField.setAccessible(true);
        handlerField.set(transportAction, jobHandler);
    }

    public void testStartOperationDelegatesToHandler() throws Exception {
        PlainActionFuture<InsightsJobResponse> future = PlainActionFuture.newFuture();
        InsightsJobRequest request = new InsightsJobRequest("12h", "/_plugins/_anomaly_detection/insights/_start");

        doAnswer(invocation -> {
            ActionListener<InsightsJobResponse> listener = invocation.getArgument(1);
            listener.onResponse(new InsightsJobResponse("started"));
            return null;
        }).when(jobHandler).startInsightsJob(eq("12h"), any());

        transportAction.doExecute((Task) null, request, future);
        assertEquals("started", future.actionGet().getMessage());
        verify(jobHandler, times(1)).startInsightsJob(eq("12h"), any());
    }

    public void testStopOperationDelegatesToHandler() throws Exception {
        PlainActionFuture<InsightsJobResponse> future = PlainActionFuture.newFuture();
        InsightsJobRequest request = new InsightsJobRequest("/_plugins/_anomaly_detection/insights/_stop");

        doAnswer(invocation -> {
            ActionListener<InsightsJobResponse> listener = invocation.getArgument(0);
            listener.onResponse(new InsightsJobResponse("stopped"));
            return null;
        }).when(jobHandler).stopInsightsJob(any());

        transportAction.doExecute((Task) null, request, future);
        assertEquals("stopped", future.actionGet().getMessage());
        verify(jobHandler, times(1)).stopInsightsJob(any());
    }

    public void testResultsOperationBuildsSearchRequestAndReturnsHits() throws Exception {
        InsightsJobRequest request = new InsightsJobRequest("det-1", "index-a", 1, 5, "/_plugins/_anomaly_detection/insights/_results");

        SearchResponse searchResponse = buildSearchResponse("{\"field\":\"value\"}", 3L);
        ArgumentCaptor<SearchRequest> searchCaptor = ArgumentCaptor.forClass(SearchRequest.class);

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(searchCaptor.capture(), any());

        PlainActionFuture<InsightsJobResponse> future = PlainActionFuture.newFuture();
        transportAction.doExecute((Task) null, request, future);

        InsightsJobResponse response = future.actionGet();
        assertEquals(3L, response.getTotalHits());
        assertEquals(1, response.getResults().size());

        SearchRequest captured = searchCaptor.getValue();
        assertArrayEquals(new String[] { ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS }, captured.indices());

        SearchSourceBuilder sourceBuilder = captured.source();
        assertEquals(1, sourceBuilder.from());
        assertEquals(5, sourceBuilder.size());
        assertEquals(1, sourceBuilder.sorts().size());
        FieldSortBuilder sortBuilder = (FieldSortBuilder) sourceBuilder.sorts().get(0);
        assertEquals("generated_at", sortBuilder.getFieldName());
        assertEquals(SortOrder.DESC, sortBuilder.order());

        assertTrue(sourceBuilder.query() instanceof BoolQueryBuilder);
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) sourceBuilder.query();
        assertEquals(2, boolQuery.must().size());
    }

    public void testResultsOperationHandlesMissingMappingGracefully() throws Exception {
        InsightsJobRequest request = new InsightsJobRequest("det-1", "index-a", 0, 10, "/_plugins/_anomaly_detection/insights/_results");

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("No mapping found for [generated_at] in order to sort on"));
            return null;
        }).when(client).search(any(), any());

        PlainActionFuture<InsightsJobResponse> future = PlainActionFuture.newFuture();
        transportAction.doExecute((Task) null, request, future);

        InsightsJobResponse response = future.actionGet();
        assertEquals(0L, response.getTotalHits());
        assertTrue(response.getResults().isEmpty());
    }

    public void testResultsOperationPropagatesSearchFailures() throws Exception {
        InsightsJobRequest request = new InsightsJobRequest("det-1", "index-a", 0, 10, "/_plugins/_anomaly_detection/insights/_results");
        RuntimeException error = new RuntimeException("boom");

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onFailure(error);
            return null;
        }).when(client).search(any(), any());

        PlainActionFuture<InsightsJobResponse> future = PlainActionFuture.newFuture();
        transportAction.doExecute((Task) null, request, future);

        RuntimeException thrown = expectThrows(RuntimeException.class, future::actionGet);
        assertSame(error, thrown);
    }

    public void testUnknownOperationFails() {
        PlainActionFuture<InsightsJobResponse> future = PlainActionFuture.newFuture();
        InsightsJobRequest request = new InsightsJobRequest("12h", "/_plugins/_anomaly_detection/insights/unsupported");

        transportAction.doExecute((Task) null, request, future);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertEquals("Unknown operation", exception.getMessage());
    }

    private SearchResponse buildSearchResponse(String source, long totalHits) {
        SearchHit hit = new SearchHit(0);
        hit.sourceRef(new BytesArray(source));
        SearchHits hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections sections = new SearchResponseSections(hits, InternalAggregations.EMPTY, null, false, null, null, 1);
        return new SearchResponse(sections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, Clusters.EMPTY);
    }
}
