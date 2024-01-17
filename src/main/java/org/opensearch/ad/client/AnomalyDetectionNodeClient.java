/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.client;

import java.util.function.Function;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.transport.GetAnomalyDetectorAction;
import org.opensearch.ad.transport.GetAnomalyDetectorRequest;
import org.opensearch.ad.transport.GetAnomalyDetectorResponse;
import org.opensearch.ad.transport.SearchAnomalyDetectorAction;
import org.opensearch.ad.transport.SearchAnomalyResultAction;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;

public class AnomalyDetectionNodeClient implements AnomalyDetectionClient {
    private final Client client;

    public AnomalyDetectionNodeClient(Client client) {
        this.client = client;
    }

    @Override
    public void searchAnomalyDetectors(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        this.client.execute(SearchAnomalyDetectorAction.INSTANCE, searchRequest, ActionListener.wrap(searchResponse -> {
            listener.onResponse(searchResponse);
        }, listener::onFailure));
    }

    @Override
    public void searchAnomalyResults(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        this.client.execute(SearchAnomalyResultAction.INSTANCE, searchRequest, ActionListener.wrap(searchResponse -> {
            listener.onResponse(searchResponse);
        }, listener::onFailure));
    }

    @Override
    public void getDetectorProfile(GetAnomalyDetectorRequest profileRequest, ActionListener<GetAnomalyDetectorResponse> listener) {
        this.client.execute(GetAnomalyDetectorAction.INSTANCE, profileRequest, getAnomalyDetectorResponseActionListener(listener));
    }

    // We need to wrap AD-specific response type listeners around an internal listener, and re-generate the response from a generic
    // ActionResponse. This is needed to prevent classloader issues and ClassCastExceptions when executed by other plugins.
    private ActionListener<GetAnomalyDetectorResponse> getAnomalyDetectorResponseActionListener(
        ActionListener<GetAnomalyDetectorResponse> listener
    ) {
        ActionListener<GetAnomalyDetectorResponse> internalListener = ActionListener.wrap(getAnomalyDetectorResponse -> {
            listener.onResponse(getAnomalyDetectorResponse);
        }, listener::onFailure);
        ActionListener<GetAnomalyDetectorResponse> actionListener = wrapActionListener(internalListener, actionResponse -> {
            GetAnomalyDetectorResponse response = GetAnomalyDetectorResponse.fromActionResponse(actionResponse);
            return response;
        });
        return actionListener;
    }

    private <T extends ActionResponse> ActionListener<T> wrapActionListener(
        final ActionListener<T> listener,
        final Function<ActionResponse, T> recreate
    ) {
        ActionListener<T> actionListener = ActionListener.wrap(r -> {
            listener.onResponse(recreate.apply(r));
            ;
        }, e -> { listener.onFailure(e); });
        return actionListener;
    }
}
