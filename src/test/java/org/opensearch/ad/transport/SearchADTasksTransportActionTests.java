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


@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
@Ignore
public class SearchADTasksTransportActionTests extends HistoricalAnalysisIntegTestCase {

    private Instant startTime;
    private Instant endTime;
    private String type = "error";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        startTime = Instant.now().minus(10, ChronoUnit.DAYS);
        endTime = Instant.now();
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type, 2000);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings
            .builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(MAX_BATCH_TASK_PER_NODE.getKey(), 1)
            .build();
    }

    public void testSearchWithoutTaskIndex() {
        SearchRequest request = searchRequest(false);
        expectThrows(IndexNotFoundException.class, () -> client().execute(SearchADTasksAction.INSTANCE, request).actionGet(10000));
    }

    public void testSearchWithNoTasks() throws IOException {
        createDetectionStateIndex();
        SearchRequest request = searchRequest(false);
        SearchResponse response = client().execute(SearchADTasksAction.INSTANCE, request).actionGet(10000);
        assertEquals(0, response.getHits().getTotalHits().value);
    }

    @Ignore
    public void testSearchWithExistingTask() throws IOException {
        startHistoricalAnalysis(startTime, endTime);
        SearchRequest searchRequest = searchRequest(true);
        SearchResponse response = client().execute(SearchADTasksAction.INSTANCE, searchRequest).actionGet(10000);
        assertEquals(1, response.getHits().getTotalHits().value);
    }

    private SearchRequest searchRequest(boolean isLatest) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(ADTask.IS_LATEST_FIELD, isLatest));
        sourceBuilder.query(query);
        SearchRequest request = new SearchRequest().source(sourceBuilder).indices(CommonName.DETECTION_STATE_INDEX);
        return request;
    }

}
*/
