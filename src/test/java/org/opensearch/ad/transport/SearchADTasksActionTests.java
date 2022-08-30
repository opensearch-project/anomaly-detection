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


@Ignore // Transport Action does not exist
public class SearchADTasksActionTests extends HistoricalAnalysisIntegTestCase {

    @Test
    public void testSearchADTasksAction() throws IOException {
        createDetectionStateIndex();
        String adTaskId = createADTask(TestHelpers.randomAdTask());

        SearchResponse searchResponse = client().execute(SearchADTasksAction.INSTANCE, matchAllRequest()).actionGet(10000);
        assertEquals(1, searchResponse.getInternalResponse().hits().getTotalHits().value);
        assertEquals(adTaskId, searchResponse.getInternalResponse().hits().getAt(0).getId());
    }

    @Test
    public void testNoIndex() {
        deleteIndexIfExists(CommonName.DETECTION_STATE_INDEX);
        SearchResponse searchResponse = client().execute(SearchADTasksAction.INSTANCE, matchAllRequest()).actionGet(10000);
        assertEquals(0, searchResponse.getInternalResponse().hits().getTotalHits().value);
    }

}
*/
