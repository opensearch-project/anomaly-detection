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
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.ad.transport.handler;

import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_SEARCH;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.ad.util.ParseUtils.addUserBackendRolesFilter;
import static org.opensearch.ad.util.ParseUtils.getUserContext;
import static org.opensearch.ad.util.RestHandlerUtils.wrapRestActionListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;

/**
 * Handle general search request, check user role and return search response.
 */
public class ADSearchHandler {
    private final Logger logger = LogManager.getLogger(ADSearchHandler.class);
    private final Client client;
    private volatile Boolean filterEnabled;

    public ADSearchHandler(Settings settings, ClusterService clusterService, Client client) {
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
        User user = getUserContext(client);
        ActionListener<SearchResponse> listener = wrapRestActionListener(actionListener, FAIL_TO_SEARCH);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            validateRole(request, user, listener);
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void validateRole(SearchRequest request, User user, ActionListener<SearchResponse> listener) {
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
