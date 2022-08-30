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

/*
package org.opensearch.ad.transport;


@Ignore
public class SearchAnomalyResultActionTests extends HistoricalAnalysisIntegTestCase {
    private SearchAnomalyResultTransportAction action;
    private TransportService transportService;
    private ThreadPool threadPool;
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
            1,
            0,
            Collections.emptyMap(),
            DocValueFormat.RAW,
            1,
            false,
            0,
            ImmutableList.of(),
            0
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
            1,
            0,
            Collections.emptyMap(),
            DocValueFormat.RAW,
            1,
            false,
            0,
            null,
            0
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
        action.multiSearch(Arrays.asList("test"), mock(SearchRequest.class), mock(PlainActionFuture.class), false);

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
            1,
            0,
            Collections.emptyMap(),
            DocValueFormat.RAW,
            1,
            false,
            0,
            createBuckets(),
            0
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
*/
