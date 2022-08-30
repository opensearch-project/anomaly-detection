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

/*package org.opensearch.ad.transport;


public class DeleteAnomalyResultsTransportActionTests extends HistoricalAnalysisIntegTestCase {

    // TODO: fix flaky test
    @Ignore
    public void testDeleteADResultAction() throws IOException, InterruptedException {
        createADResultIndex();
        String adResultId = createADResult(TestHelpers.randomAnomalyDetectResult());

        SearchResponse searchResponse = client().execute(SearchAnomalyResultAction.INSTANCE, matchAllRequest()).actionGet(10000);
        assertEquals(1, searchResponse.getInternalResponse().hits().getTotalHits().value);

        assertEquals(adResultId, searchResponse.getInternalResponse().hits().getAt(0).getId());
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(ANOMALY_RESULT_INDEX_ALIAS);
        deleteByQueryRequest.setQuery(new BoolQueryBuilder().filter(new MatchAllQueryBuilder()));
        BulkByScrollResponse deleteADResultResponse = client()
            .execute(DeleteAnomalyResultsAction.INSTANCE, deleteByQueryRequest)
            .actionGet(20000);
        waitUntil(() -> {
            SearchResponse response = client().execute(SearchAnomalyResultAction.INSTANCE, matchAllRequest()).actionGet(10000);
            return response.getInternalResponse().hits().getTotalHits().value == 0;
        }, 90, TimeUnit.SECONDS);
        assertEquals(1, deleteADResultResponse.getDeleted());
    }
}*/
