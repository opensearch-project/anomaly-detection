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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;

public class ClientUtil {
    private Client client;

    @Inject
    public ClientUtil(Client client) {
        this.client = client;
    }

    /**
     * Send an asynchronous request and handle response with the provided listener.
     * @param <Request> ActionRequest
     * @param <Response> ActionResponse
     * @param request request body
     * @param consumer request method, functional interface to operate as a client request like client::get
     * @param listener needed to handle response
     */
    public <Request extends ActionRequest, Response extends ActionResponse> void asyncRequest(
        Request request,
        BiConsumer<Request, ActionListener<Response>> consumer,
        ActionListener<Response> listener
    ) {
        consumer
            .accept(
                request,
                ActionListener.wrap(response -> { listener.onResponse(response); }, exception -> { listener.onFailure(exception); })
            );
    }

    /**
     * Execute a transport action and handle response with the provided listener.
     * @param <Request> ActionRequest
     * @param <Response> ActionResponse
     * @param action transport action
     * @param request request body
     * @param listener needed to handle response
     */
    public <Request extends ActionRequest, Response extends ActionResponse> void execute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        client
            .execute(
                action,
                request,
                ActionListener.wrap(response -> { listener.onResponse(response); }, exception -> { listener.onFailure(exception); })
            );
    }
}
