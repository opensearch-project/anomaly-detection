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

import static org.opensearch.ad.constant.ADCommonMessages.FAIL_TO_DELETE_AD_RESULT;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES;
import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.security.spi.resources.client.ResourceSharingClient;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.resources.ResourceSharingClientAccessor;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.PluginClient;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class DeleteAnomalyResultsTransportAction extends HandledTransportAction<DeleteByQueryRequest, BulkByScrollResponse> {

    private final Client client;
    private final PluginClient pluginClient;
    private volatile Boolean filterEnabled;
    private final boolean shouldUseResourceAuthz;
    private static final Logger logger = LogManager.getLogger(DeleteAnomalyResultsTransportAction.class);

    @Inject
    public DeleteAnomalyResultsTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Settings settings,
        ClusterService clusterService,
        Client client,
        PluginClient pluginClient
    ) {
        super(DeleteAnomalyResultsAction.NAME, transportService, actionFilters, DeleteByQueryRequest::new);
        this.client = client;
        this.pluginClient = pluginClient;
        this.shouldUseResourceAuthz = ParseUtils.shouldUseResourceAuthz();
        filterEnabled = AD_FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_FILTER_BY_BACKEND_ROLES, it -> filterEnabled = it);
    }

    @Override
    protected void doExecute(Task task, DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> actionListener) {
        ActionListener<BulkByScrollResponse> listener = wrapRestActionListener(actionListener, FAIL_TO_DELETE_AD_RESULT);
        delete(request, listener);
    }

    public void delete(DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
        User user = ParseUtils.getUserContext(client);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            validateRole(request, user, listener);
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void validateRole(DeleteByQueryRequest request, User user, ActionListener<BulkByScrollResponse> listener) {
        if (user == null || (!filterEnabled && !shouldUseResourceAuthz)) {
            // Case 1: user == null when 1. Security is disabled. 2. When user is super-admin
            // Case 2: If Security is enabled and filter is disabled and resource-sharing is also disabled, proceed with search.
            client.execute(DeleteByQueryAction.INSTANCE, request, listener);
        } else {
            try {
                // Security is enabled and resource sharing access control is enabled
                if (shouldUseResourceAuthz) {
                    addAccessibleConfigsFilterAndDelete(ADIndex.CONFIG.getIndexName(), request.getSearchRequest(), listener);
                    return;
                }
                // Security is enabled and backend role filter is enabled
                if (filterEnabled) {
                    ParseUtils.addUserBackendRolesFilter(user, request.getSearchRequest().source());
                }
                client.execute(DeleteByQueryAction.INSTANCE, request, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    private void addAccessibleConfigsFilterAndDelete(
        String indexName,
        SearchRequest request,
        ActionListener<BulkByScrollResponse> listener
    ) {
        logger.debug("Filtering result by accessible resources");
        ResourceSharingClient resourceSharingClient = ResourceSharingClientAccessor.getInstance().getResourceSharingClient();
        SearchSourceBuilder searchSourceBuilder = request.source();
        resourceSharingClient.getAccessibleResourceIds(indexName, ActionListener.wrap(configIds -> {
            searchSourceBuilder.query(mergeWithAccessFilter(searchSourceBuilder.query(), configIds));
            client.execute(DeleteByQueryAction.INSTANCE, request, listener);
        }, failure -> {
            // do nothing to the source or return empty set?
            searchSourceBuilder.query(QueryBuilders.boolQuery().mustNot(QueryBuilders.matchAllQuery()));
            client.execute(DeleteByQueryAction.INSTANCE, request, listener);
        }));
    }

    public static QueryBuilder mergeWithAccessFilter(QueryBuilder existing, Set<String> configIds) {
        QueryBuilder accessFilter = (configIds == null || configIds.isEmpty())
            ? QueryBuilders.boolQuery().mustNot(QueryBuilders.matchAllQuery()) // deny-all
            : QueryBuilders.termsQuery("detector_id", configIds); // allow listed detector_ids

        if (existing == null)
            return QueryBuilders.boolQuery().filter(accessFilter);
        if (existing instanceof BoolQueryBuilder) {
            ((BoolQueryBuilder) existing).filter(accessFilter);
            return existing;
        }
        return QueryBuilders.boolQuery().must(existing).filter(accessFilter);
    }

}
