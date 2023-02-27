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

package org.opensearch.ad.cluster.diskcleanup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.store.StoreStats;

@Ignore
public class IndexCleanupTests extends AbstractADTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    Client client;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    ClusterService clusterService;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    ClientUtil clientUtil;

    IndexCleanup indexCleanup;

    @Mock
    IndicesStatsResponse indicesStatsResponse;

    @Mock
    ShardStats shardStats;

    @Mock
    CommonStats commonStats;

    @Mock
    StoreStats storeStats;

    @Mock
    IndicesAdminClient indicesAdminClient;

    @SuppressWarnings("unchecked")
    @Override
    public void setUp() throws Exception {
        super.setUp();

        MockitoAnnotations.initMocks(this);
        when(clusterService.state().getRoutingTable().hasIndex(anyString())).thenReturn(true);
        super.setUpLog4jForJUnit(IndexCleanup.class);
        indexCleanup = new IndexCleanup(client, clientUtil, clusterService);
        when(indicesStatsResponse.getShards()).thenReturn(new ShardStats[] { shardStats });
        when(shardStats.getStats()).thenReturn(commonStats);
        when(commonStats.getStore()).thenReturn(storeStats);
        when(client.admin().indices()).thenReturn(indicesAdminClient);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<IndicesStatsResponse> listener = (ActionListener<IndicesStatsResponse>) args[1];
            listener.onResponse(indicesStatsResponse);
            return null;
        }).when(indicesAdminClient).stats(any(), any());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
    }

    public void testDeleteDocsBasedOnShardSizeWithCleanupNeededAsTrue() throws Exception {
        long maxShardSize = 1000;
        when(storeStats.getSizeInBytes()).thenReturn(maxShardSize + 1);
        indexCleanup.deleteDocsBasedOnShardSize("indexname", maxShardSize, null, ActionListener.wrap(result -> {
            assertTrue(result);
            verify(clientUtil).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());
        }, exception -> { throw new RuntimeException(exception); }));
    }

    public void testDeleteDocsBasedOnShardSizeWithCleanupNeededAsFalse() throws Exception {
        long maxShardSize = 1000;
        when(storeStats.getSizeInBytes()).thenReturn(maxShardSize - 1);
        indexCleanup
            .deleteDocsBasedOnShardSize(
                "indexname",
                maxShardSize,
                null,
                ActionListener.wrap(Assert::assertFalse, exception -> { throw new RuntimeException(exception); })
            );
    }

    public void testDeleteDocsBasedOnShardSizeIndexNotExisted() throws Exception {
        when(clusterService.state().getRoutingTable().hasIndex(anyString())).thenReturn(false);
        Logger logger = (Logger) LogManager.getLogger(IndexCleanup.class);
        logger.setLevel(Level.DEBUG);
        indexCleanup.deleteDocsBasedOnShardSize("indexname", 1000, null, null);
        assertTrue(testAppender.containsMessage("skip as the index:indexname doesn't exist"));
    }
}
