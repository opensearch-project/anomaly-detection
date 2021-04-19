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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.util;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;

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

    private Client client;
    private ClientUtil clientUtil;
    private ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

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
        Client client,
        ClientUtil clientUtil,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        this.client = client;
        this.clientUtil = clientUtil;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
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
    @Deprecated
    public Long getNumberOfDocumentsInIndex(String indexName) {
        if (!clusterService.state().getRoutingTable().hasIndex(indexName)) {
            return 0L;
        }
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        Optional<IndicesStatsResponse> response = clientUtil.timedRequest(indicesStatsRequest, logger, client.admin().indices()::stats);
        return response.map(r -> r.getIndex(indexName).getPrimaries().docs.getCount()).orElse(-1L);
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
