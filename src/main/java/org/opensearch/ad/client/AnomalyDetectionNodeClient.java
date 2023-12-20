/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.client;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.transport.ProfileAction;
import org.opensearch.ad.transport.ProfileRequest;
import org.opensearch.ad.transport.ProfileResponse;
import org.opensearch.ad.transport.SearchAnomalyDetectorAction;
import org.opensearch.ad.transport.SearchAnomalyResultAction;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;

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
    public void getDetectorProfile(ProfileRequest profileRequest, ActionListener<ProfileResponse> listener) {
        this.client
            .execute(
                ProfileAction.INSTANCE,
                profileRequest,
                ActionListener.wrap(profileResponse -> { listener.onResponse(profileResponse); }, listener::onFailure)
            );
    }
}
