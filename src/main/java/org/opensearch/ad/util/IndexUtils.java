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

package org.opensearch.ad.util;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.indices.IndicesStatsRequest;
import org.opensearch.client.opensearch.indices.IndicesStatsResponse;
import org.opensearch.client.transport.TransportOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.inject.Inject;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;

public class IndexUtils {
    /**
     * Status string of index that does not exist
     */
    public static final String NONEXISTENT_INDEX_STATUS = "non-existent";

    /**
     * Status string when an alias exists, but does not point to an index
     */
    public static final String ALIAS_EXISTS_NO_INDICES_STATUS = "alias exists, but does not point to any indices";
    public static final String ALIAS_POINTS_TO_MULTIPLE_INDICES_STATUS = "alias exists, but does not point to any " + "indices";

    private static final Logger logger = LogManager.getLogger(IndexUtils.class);

    private SDKRestClient client;
    private ClientUtil clientUtil;
    private SDKClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    private final OpenSearchAsyncClient openSearchAsyncClient;

    /**
     * Inject annotation required by Guice to instantiate EntityResultTransportAction (transitive dependency)
     *
     * @param client Client to make calls to ElasticSearch
     * @param clientUtil AD Client utility
     * @param clusterService ES ClusterService
     * @param indexNameExpressionResolver index name resolver
     */
    @Inject
    public IndexUtils(
        SDKRestClient client,
        ClientUtil clientUtil,
        SDKClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        OpenSearchAsyncClient openSearchAsyncClient
    ) {
        this.client = client;
        this.clientUtil = clientUtil;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.openSearchAsyncClient = openSearchAsyncClient;
    }

    /**
     * Gets the cluster index health for a particular index or the index an alias points to
     *
     * If an alias is passed in, it will only return the health status of an index it points to if it only points to a
     * single index. If it points to multiple indices, it will throw an exception.
     *
     * @param indexOrAliasName String of the index or alias name to get health of.
     * @return String represents the status of the index: "red", "yellow" or "green"
     * @throws IllegalArgumentException Thrown when an alias is passed in that points to more than one index
     */
    public String getIndexHealthStatus(String indexOrAliasName) throws IllegalArgumentException {
        if (!clusterService.state().getRoutingTable().hasIndex(indexOrAliasName)) {
            // Check if the index is actually an alias
            if (clusterService.state().metadata().hasAlias(indexOrAliasName)) {
                // List of all indices the alias refers to
                List<IndexMetadata> indexMetaDataList = clusterService
                    .state()
                    .metadata()
                    .getIndicesLookup()
                    .get(indexOrAliasName)
                    .getIndices();
                if (indexMetaDataList.size() == 0) {
                    return ALIAS_EXISTS_NO_INDICES_STATUS;
                } else if (indexMetaDataList.size() > 1) {
                    throw new IllegalArgumentException("Cannot get health for alias that points to multiple indices");
                } else {
                    indexOrAliasName = indexMetaDataList.get(0).getIndex().getName();
                }
            } else {
                return NONEXISTENT_INDEX_STATUS;
            }
        }

        ClusterIndexHealth indexHealth = new ClusterIndexHealth(
            clusterService.state().metadata().index(indexOrAliasName),
            clusterService.state().getRoutingTable().index(indexOrAliasName)
        );

        return indexHealth.getStatus().name().toLowerCase(Locale.ROOT);
    }

    /**
     * Gets the number of documents in an index.
     *
     * @deprecated
     *
     * @param indexName Name of the index
     * @return The number of documents in an index. 0 is returned if the index does not exist. -1 is returned if the
     * request fails.
     */
    public Long getNumberOfDocumentsInIndex(String indexName) {
        if (!clusterService.state().getRoutingTable().hasIndex(indexName)) {
            return 0L;
        }
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest.Builder().build();

        CompletableFuture<IndicesStatsResponse> indicesStatsFutureResponse = openSearchAsyncClient
            ._transport()
            .performRequestAsync(indicesStatsRequest, IndicesStatsRequest._ENDPOINT, TransportOptions.builder().build());
        // Optional<IndicesStatsResponse> response = clientUtil.timedRequest(indicesStatsRequest, logger, client.admin().indices()::stats);

        IndicesStatsResponse indicesStatsResponse;
        Long numberOfDocumentsInIndex = -1L;
        try {
            indicesStatsResponse = indicesStatsFutureResponse.orTimeout(10L, TimeUnit.SECONDS).get();
            numberOfDocumentsInIndex = indicesStatsResponse.indices().get(indexName).primaries().docs().count();
        } catch (Exception e) {
            logger.info("Could not fetch indicesStats count due to ", e);
        }
        return numberOfDocumentsInIndex;
    }

    /**
     * Similar to checkGlobalBlock, we check block on the indices level.
     *
     * @param state   Cluster state
     * @param level   block level
     * @param indices the indices on which to check block
     * @return whether any of the index has block on the level.
     */
    public boolean checkIndicesBlocked(ClusterState state, ClusterBlockLevel level, String... indices) {
        // the original index might be an index expression with wildcards like "log*",
        // so we need to expand the expression to concrete index name
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), indices);

        return state.blocks().indicesBlockedException(level, concreteIndices) != null;
    }
}
