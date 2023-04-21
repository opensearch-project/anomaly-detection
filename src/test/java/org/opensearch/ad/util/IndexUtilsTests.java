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
/*  @anomaly-detection -commented until we have support for SDKRestClient.stats() https://github.com/opensearch-project/opensearch-sdk-java/issues/620
package org.opensearch.ad.util;


public class IndexUtilsTests extends OpenSearchIntegTestCase {

    private ClientUtil clientUtil;

    private IndexNameExpressionResolver indexNameResolver;

    @Before
    public void setup() {
        Client client = client();
        Clock clock = mock(Clock.class);
        Throttler throttler = new Throttler(clock);
        clientUtil = new ClientUtil(Settings.EMPTY, client, throttler);
        indexNameResolver = mock(IndexNameExpressionResolver.class);
    }

    @Test
    public void testGetIndexHealth_NoIndex() {
        IndexUtils indexUtils = new IndexUtils(client(), clientUtil, clusterService(), indexNameResolver);
        String output = indexUtils.getIndexHealthStatus("test");
        assertEquals(IndexUtils.NONEXISTENT_INDEX_STATUS, output);
    }

    @Test
    public void testGetIndexHealth_Index() {
        String indexName = "test-2";
        createIndex(indexName);
        flush();
        IndexUtils indexUtils = new IndexUtils(client(), clientUtil, clusterService(), indexNameResolver);
        String status = indexUtils.getIndexHealthStatus(indexName);
        assertTrue(status.equals("green") || status.equals("yellow"));
    }

    @Test
    public void testGetIndexHealth_Alias() {
        String indexName = "test-2";
        String aliasName = "alias";
        createIndex(indexName);
        flush();
        AcknowledgedResponse response = client().admin().indices().prepareAliases().addAlias(indexName, aliasName).execute().actionGet();
        assertTrue(response.isAcknowledged());
        IndexUtils indexUtils = new IndexUtils(client(), clientUtil, clusterService(), indexNameResolver);
        String status = indexUtils.getIndexHealthStatus(aliasName);
        assertTrue(status.equals("green") || status.equals("yellow"));
    }

    @Test
    public void testGetNumberOfDocumentsInIndex_NonExistentIndex() {
        IndexUtils indexUtils = new IndexUtils(client(), clientUtil, clusterService(), indexNameResolver);
        assertEquals((Long) 0L, indexUtils.getNumberOfDocumentsInIndex("index"));
    }

    @Test
    public void testGetNumberOfDocumentsInIndex_RegularIndex() {
        String indexName = "test-2";
        createIndex(indexName);
        flush();

        long count = 2100;
        for (int i = 0; i < count; i++) {
            index(indexName, "_doc", String.valueOf(i), "{}");
        }
        flushAndRefresh(indexName);
        IndexUtils indexUtils = new IndexUtils(client(), clientUtil, clusterService(), indexNameResolver);
        assertEquals((Long) count, indexUtils.getNumberOfDocumentsInIndex(indexName));
    }
}
*/
