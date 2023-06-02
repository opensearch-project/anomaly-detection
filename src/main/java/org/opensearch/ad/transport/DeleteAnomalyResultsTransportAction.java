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
import static org.opensearch.ad.settings.AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.ad.util.RestHandlerUtils.wrapRestActionListener;
import static org.opensearch.timeseries.util.ParseUtils.addUserBackendRolesFilter;
import static org.opensearch.timeseries.util.ParseUtils.getUserContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class DeleteAnomalyResultsTransportAction extends HandledTransportAction<DeleteByQueryRequest, BulkByScrollResponse> {

    private final Client client;
    private volatile Boolean filterEnabled;
    private static final Logger logger = LogManager.getLogger(DeleteAnomalyResultsTransportAction.class);

    @Inject
    public DeleteAnomalyResultsTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Settings settings,
        ClusterService clusterService,
        Client client
    ) {
        super(DeleteAnomalyResultsAction.NAME, transportService, actionFilters, DeleteByQueryRequest::new);
        this.client = client;
        filterEnabled = FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterEnabled = it);
    }

    @Override
    protected void doExecute(Task task, DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> actionListener) {
        ActionListener<BulkByScrollResponse> listener = wrapRestActionListener(actionListener, FAIL_TO_DELETE_AD_RESULT);
        delete(request, listener);
    }

    public void delete(DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
        User user = getUserContext(client);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            validateRole(request, user, listener);
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void validateRole(DeleteByQueryRequest request, User user, ActionListener<BulkByScrollResponse> listener) {
        if (user == null || !filterEnabled) {
            // Case 1: user == null when 1. Security is disabled. 2. When user is super-admin
            // Case 2: If Security is enabled and filter is disabled, proceed with search as
            // user is already authenticated to hit this API.
            client.execute(DeleteByQueryAction.INSTANCE, request, listener);
        } else {
            // Security is enabled and backend role filter is enabled
            try {
                addUserBackendRolesFilter(user, request.getSearchRequest().source());
                client.execute(DeleteByQueryAction.INSTANCE, request, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }
}
