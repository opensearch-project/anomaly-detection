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

package org.opensearch.ad.transport.handler;

import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_SEARCH;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.ad.util.ParseUtils.addUserBackendRolesFilter;
import static org.opensearch.ad.util.ParseUtils.getNullUser;
import static org.opensearch.ad.util.RestHandlerUtils.wrapRestActionListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.auth.UserIdentity;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;

import com.google.inject.Inject;

/**
 * Handle general search request, check user role and return search response.
 */
public class ADSearchHandler {
    private final Logger logger = LogManager.getLogger(ADSearchHandler.class);
    private final SDKRestClient client;
    private volatile Boolean filterEnabled;

    @Inject
    public ADSearchHandler(Settings settings, SDKClusterService clusterService, SDKRestClient client) {
        this.client = client;
        filterEnabled = AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterEnabled = it);
    }

    /**
     * Validate user role, add backend role filter if filter enabled
     * and execute search.
     *
     * @param request search request
     * @param actionListener action listerner
     */
    public void search(SearchRequest request, ActionListener<SearchResponse> actionListener) {
        // Temporary null user for AD extension without security. Will always validate role.
        UserIdentity user = getNullUser();
        ActionListener<SearchResponse> listener = wrapRestActionListener(actionListener, FAIL_TO_SEARCH);
        try {
            validateRole(request, user, listener);
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void validateRole(SearchRequest request, UserIdentity user, ActionListener<SearchResponse> listener) {
        if (user == null || !filterEnabled) {
            // Case 1: user == null when 1. Security is disabled. 2. When user is super-admin
            // Case 2: If Security is enabled and filter is disabled, proceed with search as
            // user is already authenticated to hit this API.
            client.search(request, listener);
        } else {
            // Security is enabled and filter is enabled
            try {
                addUserBackendRolesFilter(user, request.source());
                logger.debug("Filtering result by " + user.getBackendRoles());
                client.search(request, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

}
