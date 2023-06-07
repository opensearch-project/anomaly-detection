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
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.indices.IndicesStatsRequest;
import org.opensearch.client.opensearch.indices.IndicesStatsResponse;
import org.opensearch.client.transport.TransportOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.settings.Settings;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;

import com.google.inject.Inject;

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

    private SDKRestClient sdkRestClient;
    private ClientUtil clientUtil;
    private SDKClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Settings settings;
    private final OpenSearchAsyncClient javaAsyncClient;

    /**
     * Inject annotation required by Guice to instantiate EntityResultTransportAction (transitive dependency)
     *
     * @param sdkRestClient SdkRestClient to make calls to ElasticSearch
     * @param clientUtil AD Client utility
     * @param clusterService ES ClusterService
     * @param indexNameExpressionResolver index name resolver
     * @param javaAsyncClient OpenSearchAsyncClient
     */
    @Inject
    public IndexUtils(
        SDKRestClient sdkRestClient,
        ClientUtil clientUtil,
        SDKClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        OpenSearchAsyncClient javaAsyncClient
    ) {
        this(sdkRestClient, clientUtil, clusterService, indexNameExpressionResolver, javaAsyncClient, Settings.EMPTY);
    }

    /**
     * Instantiates a new IndexUtils object
     *
     * @param sdkRestClient SdkRestClient to make calls to ElasticSearch
     * @param clientUtil AD Client utility
     * @param clusterService ES ClusterService
     * @param indexNameExpressionResolver index name resolver
     * @param javaAsyncClient OpenSearchAsyncClient
     * @param settings Environment Settings
     */
    public IndexUtils(
        SDKRestClient sdkRestClient,
        ClientUtil clientUtil,
        SDKClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        OpenSearchAsyncClient javaAsyncClient,
        Settings settings
    ) {
        this.sdkRestClient = sdkRestClient;
        this.clientUtil = clientUtil;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.javaAsyncClient = javaAsyncClient;
        this.settings = settings;
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
        if (!indexExists(indexOrAliasName)) {
            // Check if the index is actually an alias
            if (aliasExists(indexOrAliasName)) {
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

        ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest(indexOrAliasName);
        CompletableFuture<ClusterHealthResponse> clusterHealthFuture = new CompletableFuture<>();
        sdkRestClient
            .cluster()
            .health(clusterHealthRequest, ActionListener.wrap(response -> { clusterHealthFuture.complete(response); }, exception -> {
                clusterHealthFuture.completeExceptionally(exception);
            }));

        ClusterHealthResponse clusterHealthResponse = clusterHealthFuture
            .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings).getMillis(), TimeUnit.MILLISECONDS)
            .join();

        ClusterIndexHealth indexHealth = clusterHealthResponse.getIndices().get(indexOrAliasName);

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
        if (!indexExists(indexName)) {
            return 0L;
        }
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest.Builder().build();

        CompletableFuture<IndicesStatsResponse> indicesStatsFutureResponse = javaAsyncClient
            ._transport()
            .performRequestAsync(indicesStatsRequest, IndicesStatsRequest._ENDPOINT, TransportOptions.builder().build());

        IndicesStatsResponse indicesStatsResponse;
        Long numberOfDocumentsInIndex = -1L;
        try {
            indicesStatsResponse = indicesStatsFutureResponse
                .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings).getMillis(), TimeUnit.MILLISECONDS)
                .get();
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

    private boolean indexExists(String indexName) {
        GetIndexRequest getindexRequest = new GetIndexRequest(indexName);

        CompletableFuture<Boolean> existsFuture = new CompletableFuture<>();
        sdkRestClient.indices().exists(getindexRequest, ActionListener.wrap(response -> { existsFuture.complete(response); }, exception -> {
            existsFuture.completeExceptionally(exception);
        }));

        Boolean existsResponse = existsFuture
            .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings).getMillis(), TimeUnit.MILLISECONDS)
            .join();

        return existsResponse.booleanValue();
    }

    private boolean aliasExists(String aliasName) {
        GetAliasesRequest getAliasRequest = new GetAliasesRequest(aliasName);

        CompletableFuture<Boolean> existsFuture = new CompletableFuture<>();
        sdkRestClient
            .indices()
            .existsAlias(getAliasRequest, ActionListener.wrap(response -> { existsFuture.complete(response); }, exception -> {
                existsFuture.completeExceptionally(exception);
            }));

        Boolean existsResponse = existsFuture
            .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings).getMillis(), TimeUnit.MILLISECONDS)
            .join();

        return existsResponse.booleanValue();
    }
}
