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

package org.opensearch.timeseries.transport.handler;

import static org.opensearch.timeseries.util.ParseUtils.isAdmin;
import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.transport.client.Client;

/**
 * Handle general search request, check user role and return search response.
 */
public class SearchHandler {
    private final Logger logger = LogManager.getLogger(SearchHandler.class);
    private final Client client;
    private volatile Boolean filterEnabled;
    private final boolean shouldUseResourceAuthz;

    public SearchHandler(Settings settings, ClusterService clusterService, Client client, Setting<Boolean> filterByBackendRoleSetting) {
        this.client = client;
        filterEnabled = filterByBackendRoleSetting.get(settings);
        shouldUseResourceAuthz = ParseUtils.shouldUseResourceAuthz(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(filterByBackendRoleSetting, it -> filterEnabled = it);
    }

    /**
     * Validate user role, add backend role filter if filter enabled
     * and execute search.
     *
     * @param request        search request
     * @param pair          paid of index name and id field
     * @param actionListener action listerner
     */
    public void search(SearchRequest request, Pair<String, String> pair, ActionListener<SearchResponse> actionListener) {
        User user = ParseUtils.getUserContext(client);
        ActionListener<SearchResponse> listener = wrapRestActionListener(actionListener, CommonMessages.FAIL_TO_SEARCH);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            if (shouldUseResourceAuthz) {
                ParseUtils.addAccessibleConfigsFilterAndSearch(client, pair, request, listener);
            } else {
                validateRole(request, user, listener);
            }
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void validateRole(SearchRequest request, User user, ActionListener<SearchResponse> listener) {
        if (user == null || !filterEnabled || isAdmin(user)) {
            // Case 1: user == null when 1. Security is disabled. 2. When user is super-admin
            // Case 2: If Security is enabled and filter is disabled, proceed with search as
            // user is already authenticated to hit this API.
            // case 3: user is admin which means we don't have to check backend role filtering
            client.search(request, listener);
        } else {
            // Security is enabled, filter is enabled and user isn't admin
            try {
                ParseUtils.addUserBackendRolesFilter(user, request.source());
                logger.debug("Filtering result by " + user.getBackendRoles());
                client.search(request, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

}
