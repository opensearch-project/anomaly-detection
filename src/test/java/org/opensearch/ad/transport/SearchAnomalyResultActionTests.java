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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.TestHelpers.createClusterState;
import static org.opensearch.ad.TestHelpers.createSearchResponse;
import static org.opensearch.ad.TestHelpers.matchAllRequest;
import static org.opensearch.ad.indices.AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.HistoricalAnalysisIntegTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.handler.ADSearchHandler;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableList;

public class SearchAnomalyResultActionTests extends HistoricalAnalysisIntegTestCase {
    private SearchAnomalyResultTransportAction action;
    private TransportService transportService;
    private ThreadPool threadPool;
    private ThreadContext threadContext;
    private Client client;
    private ClusterService clusterService;
    private ActionFilters actionFilters;
    private ADSearchHandler searchHandler;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private PlainActionFuture<SearchResponse> future;
    private ClusterState clusterState;
    private SearchResponse searchResponse;
    private MultiSearchResponse multiSearchResponse;
    private StringTerms resultIndicesAgg;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        clusterState = createClusterState();
        when(clusterService.state()).thenReturn(clusterState);

        transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );

        client = mock(Client.class);
        threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        Settings settings = Settings.builder().build();
        threadContext = new ThreadContext(settings);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        actionFilters = mock(ActionFilters.class);
        searchHandler = mock(ADSearchHandler.class);
        indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
        action = new SearchAnomalyResultTransportAction(
            transportService,
            actionFilters,
            searchHandler,
            clusterService,
            indexNameExpressionResolver,
            client
        );
    }

    @Test
    public void testSearchAnomalyResult_NoIndices() {
        future = mock(PlainActionFuture.class);
        SearchRequest request = new SearchRequest().indices(new String[] {});

        action.doExecute(mock(Task.class), request, future);
        verify(future).onFailure(any(IllegalArgumentException.class));
    }

    @Test
    public void testSearchAnomalyResult_NullAggregationInSearchResponse() {
        future = mock(PlainActionFuture.class);
        SearchRequest request = new SearchRequest().indices(new String[] { "opensearch-ad-plugin-result-test" });
        when(indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandOpen(), request.indices()))
            .thenReturn(new String[] { "opensearch-ad-plugin-result-test" });

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            listener.onResponse(createSearchResponse(TestHelpers.randomAnomalyDetectResult(0.87)));
            return null;
        }).when(client).search(any(), any());

        action.doExecute(mock(Task.class), request, future);
        verify(client).search(any(), any());
    }

    @Test
    public void testSearchAnomalyResult_EmptyBucketsInSearchResponse() {
        searchResponse = mock(SearchResponse.class);
        resultIndicesAgg = new StringTerms(
            "result_index",
            InternalOrder.key(false),
            BucketOrder.count(false),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            1,
            false,
            0,
            ImmutableList.of(),
            0,
            new TermsAggregator.BucketCountThresholds(1, 0, 1, 0)
        );
        List<Aggregation> list = new ArrayList<>();
        list.add(resultIndicesAgg);
        Aggregations aggregations = new Aggregations(list);

        when(searchResponse.getAggregations()).thenReturn(aggregations);

        action
            .processSingleSearchResponse(
                searchResponse,
                mock(SearchRequest.class),
                mock(PlainActionFuture.class),
                new HashSet<>(),
                new ArrayList<>()
            );
        verify(searchHandler).search(any(), any());
    }

    @Test
    public void testSearchAnomalyResult_NullBucketsInSearchResponse() {
        searchResponse = mock(SearchResponse.class);
        resultIndicesAgg = new StringTerms(
            "result_index",
            InternalOrder.key(false),
            BucketOrder.count(false),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            1,
            false,
            0,
            null,
            0,
            new TermsAggregator.BucketCountThresholds(1, 0, 1, 0)
        );
        List<Aggregation> list = new ArrayList<>();
        list.add(resultIndicesAgg);
        Aggregations aggregations = new Aggregations(list);

        when(searchResponse.getAggregations()).thenReturn(aggregations);

        action
            .processSingleSearchResponse(
                searchResponse,
                mock(SearchRequest.class),
                mock(PlainActionFuture.class),
                new HashSet<>(),
                new ArrayList<>()
            );
        verify(searchHandler).search(any(), any());
    }

    @Test
    public void testMultiSearch_NoOnlyQueryCustomResultIndex() {
        action
            .multiSearch(
                Arrays.asList("test"),
                mock(SearchRequest.class),
                mock(PlainActionFuture.class),
                false,
                threadContext.stashContext()
            );

        verify(client).multiSearch(any(), any());
    }

    @Test
    public void testSearchAnomalyResult_MultiSearch() {
        future = mock(PlainActionFuture.class);
        SearchRequest request = new SearchRequest().indices(new String[] { "opensearch-ad-plugin-result-test" });
        when(indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandOpen(), request.indices()))
            .thenReturn(new String[] { "opensearch-ad-plugin-result-test" });

        searchResponse = mock(SearchResponse.class);
        resultIndicesAgg = new StringTerms(
            "result_index",
            InternalOrder.key(false),
            BucketOrder.count(false),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            1,
            false,
            0,
            createBuckets(),
            0,
            new TermsAggregator.BucketCountThresholds(1, 0, 1, 0)
        );
        List<Aggregation> list = new ArrayList<>();
        list.add(resultIndicesAgg);
        Aggregations aggregations = new Aggregations(list);

        when(searchResponse.getAggregations()).thenReturn(aggregations);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());

        multiSearchResponse = mock(MultiSearchResponse.class);
        MultiSearchResponse.Item multiSearchResponseItem = mock(MultiSearchResponse.Item.class);
        when(multiSearchResponse.getResponses()).thenReturn(new MultiSearchResponse.Item[] { multiSearchResponseItem });
        when(multiSearchResponseItem.getFailure()).thenReturn(null);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<MultiSearchResponse> listener = (ActionListener<MultiSearchResponse>) args[1];
            listener.onResponse(multiSearchResponse);
            return null;
        }).when(client).multiSearch(any(), any());

        action.doExecute(mock(Task.class), request, future);
        verify(client).search(any(), any());
        verify(client).multiSearch(any(), any());
        verify(searchHandler).search(any(), any());
    }

    @Test
    public void testSearchResultAction() throws IOException {
        createADResultIndex();
        String adResultId = createADResult(TestHelpers.randomAnomalyDetectResult());

        SearchResponse searchResponse = client()
            .execute(SearchAnomalyResultAction.INSTANCE, matchAllRequest().indices(ALL_AD_RESULTS_INDEX_PATTERN))
            .actionGet(10000);
        assertEquals(1, searchResponse.getInternalResponse().hits().getTotalHits().value);

        assertEquals(adResultId, searchResponse.getInternalResponse().hits().getAt(0).getId());
    }

    @Test
    public void testNoIndex() {
        deleteIndexIfExists(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
        SearchResponse searchResponse = client()
            .execute(SearchAnomalyResultAction.INSTANCE, matchAllRequest().indices(ALL_AD_RESULTS_INDEX_PATTERN))
            .actionGet(10000);
        assertEquals(0, searchResponse.getHits().getTotalHits().value);
    }

    private List<StringTerms.Bucket> createBuckets() {
        String entity1Name = "opensearch-ad-plugin-result-test";
        long entity1Count = 3;
        StringTerms.Bucket entity1Bucket = new StringTerms.Bucket(
            new BytesRef(entity1Name.getBytes(StandardCharsets.UTF_8), 0, entity1Name.getBytes(StandardCharsets.UTF_8).length),
            entity1Count,
            null,
            false,
            0L,
            DocValueFormat.RAW
        );
        List<StringTerms.Bucket> stringBuckets = ImmutableList.of(entity1Bucket);
        return stringBuckets;
    }
}
