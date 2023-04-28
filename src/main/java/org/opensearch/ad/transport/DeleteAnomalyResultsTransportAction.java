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

import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_DELETE_AD_RESULT;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.ad.util.ParseUtils.addUserBackendRolesFilter;
import static org.opensearch.ad.util.ParseUtils.getNullUser;
import static org.opensearch.ad.util.RestHandlerUtils.wrapRestActionListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.ad.auth.UserIdentity;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;

import com.google.inject.Inject;

public class DeleteAnomalyResultsTransportAction extends TransportAction<DeleteByQueryRequest, BulkByScrollResponse> {

    private final SDKRestClient sdkRestClient;
    private volatile Boolean filterEnabled;
    private static final Logger logger = LogManager.getLogger(DeleteAnomalyResultsTransportAction.class);
    private final Settings settings;

    @Inject
    public DeleteAnomalyResultsTransportAction(
        ExtensionsRunner extensionsRunner,
        TaskManager taskManager,
        ActionFilters actionFilters,
        SDKClusterService sdkClusterService,
        SDKRestClient sdkRestClient
    ) {
        super(DeleteAnomalyResultsAction.NAME, actionFilters, taskManager);
        this.sdkRestClient = sdkRestClient;
        this.settings = extensionsRunner.getEnvironmentSettings();
        filterEnabled = FILTER_BY_BACKEND_ROLES.get(settings);
        sdkClusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterEnabled = it);
    }

    @Override
    protected void doExecute(Task task, DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> actionListener) {
        ActionListener<BulkByScrollResponse> listener = wrapRestActionListener(actionListener, FAIL_TO_DELETE_AD_RESULT);
        delete(request, listener);
    }

    public void delete(DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
        // Temporary null user for AD extension without security. Will always validate role.
        UserIdentity user = getNullUser();
        try {
            validateRole(request, user, listener);
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void validateRole(DeleteByQueryRequest request, UserIdentity user, ActionListener<BulkByScrollResponse> listener) {
        if (user == null || !filterEnabled) {
            // Case 1: user == null when 1. Security is disabled. 2. When user is super-admin
            // Case 2: If Security is enabled and filter is disabled, proceed with search as
            // user is already authenticated to hit this API.
            // client.execute(DeleteByQueryAction.INSTANCE, request, listener);
            // client.execute(DeleteByQueryAction.INSTANCE, request, listener);
            sdkRestClient.deleteByQuery(request, listener);
        } else {
            // Security is enabled and backend role filter is enabled
            try {
                addUserBackendRolesFilter(user, request.getSearchRequest().source());
                // client.execute(DeleteByQueryAction.INSTANCE, request, listener);
                sdkRestClient.deleteByQuery(request, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }
}
