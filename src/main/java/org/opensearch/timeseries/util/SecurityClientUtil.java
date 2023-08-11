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

package org.opensearch.timeseries.util;

import java.util.function.BiConsumer;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;

public class SecurityClientUtil {
    private static final String INJECTION_ID = "direct";
    private NodeStateManager nodeStateManager;
    private Settings settings;

    @Inject
    public SecurityClientUtil(NodeStateManager nodeStateManager, Settings settings) {
        this.nodeStateManager = nodeStateManager;
        this.settings = settings;
    }

    /**
     * Send an asynchronous request in the context of user role and handle response with the provided listener. The role
     * is recorded in a detector config.
     * @param <Request> ActionRequest
     * @param <Response> ActionResponse
     * @param request request body
     * @param consumer request method, functional interface to operate as a client request like client::get
     * @param detectorId Detector id
     * @param client OpenSearch client
     * @param listener needed to handle response
     */
    public <Request extends ActionRequest, Response extends ActionResponse> void asyncRequestWithInjectedSecurity(
        Request request,
        BiConsumer<Request, ActionListener<Response>> consumer,
        String detectorId,
        Client client,
        AnalysisType context,
        ActionListener<Response> listener
    ) {
        ThreadContext threadContext = client.threadPool().getThreadContext();
        try (
            TimeSeriesSafeSecurityInjector injectSecurity = new TimeSeriesSafeSecurityInjector(
                detectorId,
                settings,
                threadContext,
                nodeStateManager,
                context
            )
        ) {
            injectSecurity
                .injectUserRolesFromConfig(
                    ActionListener
                        .wrap(
                            success -> consumer.accept(request, ActionListener.runBefore(listener, () -> injectSecurity.close())),
                            listener::onFailure
                        )
                );
        }
    }

    /**
     * Send an asynchronous request in the context of user role and handle response with the provided listener. The role
     * is provided in the arguments.
     * @param <Request> ActionRequest
     * @param <Response> ActionResponse
     * @param request request body
     * @param consumer request method, functional interface to operate as a client request like client::get
     * @param user User info
     * @param client OpenSearch client
     * @param listener needed to handle response
     */
    public <Request extends ActionRequest, Response extends ActionResponse> void asyncRequestWithInjectedSecurity(
        Request request,
        BiConsumer<Request, ActionListener<Response>> consumer,
        User user,
        Client client,
        AnalysisType context,
        ActionListener<Response> listener
    ) {
        ThreadContext threadContext = client.threadPool().getThreadContext();
        // use a hardcoded string as detector id that is only used in logging
        // Question:
        // Will the try-with-resources statement auto close injectSecurity?
        // Here the injectSecurity is closed explicitly. So we don't need to put the injectSecurity inside try ?
        // Explanation:
        // There might be two threads: one thread covers try, inject, and triggers client.execute/client.search
        // (this can be a thread in the write thread pool); another thread actually execute the logic of
        // client.execute/client.search and handles the responses (this can be a thread in the search thread pool).
        // Auto-close in try will restore the context in one thread; the explicit close injectSecurity will restore
        // the context in another thread. So we still need to put the injectSecurity inside try.
        try (
            TimeSeriesSafeSecurityInjector injectSecurity = new TimeSeriesSafeSecurityInjector(
                INJECTION_ID,
                settings,
                threadContext,
                nodeStateManager,
                context
            )
        ) {
            injectSecurity.injectUserRoles(user);
            consumer.accept(request, ActionListener.runBefore(listener, () -> injectSecurity.close()));
        }
    }

    /**
     * Execute a transport action in the context of user role and handle response with the provided listener. The role
     * is provided in the arguments.
     * @param <Request> ActionRequest
     * @param <Response> ActionResponse
     * @param action transport action
     * @param request request body
     * @param user User info
     * @param client OpenSearch client
     * @param listener needed to handle response
     */
    public <Request extends ActionRequest, Response extends ActionResponse> void executeWithInjectedSecurity(
        ActionType<Response> action,
        Request request,
        User user,
        Client client,
        AnalysisType context,
        ActionListener<Response> listener
    ) {
        ThreadContext threadContext = client.threadPool().getThreadContext();

        // use a hardcoded string as detector id that is only used in logging
        try (
            TimeSeriesSafeSecurityInjector injectSecurity = new TimeSeriesSafeSecurityInjector(
                INJECTION_ID,
                settings,
                threadContext,
                nodeStateManager,
                context
            )
        ) {
            injectSecurity.injectUserRoles(user);
            client.execute(action, request, ActionListener.runBefore(listener, () -> injectSecurity.close()));
        }
    }
}
