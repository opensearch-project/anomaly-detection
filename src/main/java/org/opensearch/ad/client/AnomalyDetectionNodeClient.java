/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.client;

import java.util.function.Function;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.transport.ADTaskProfileAction;
import org.opensearch.ad.transport.ADTaskProfileRequest;
import org.opensearch.ad.transport.ADTaskProfileResponse;
import org.opensearch.ad.transport.SearchAnomalyDetectorAction;
import org.opensearch.ad.transport.SearchAnomalyResultAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

public class AnomalyDetectionNodeClient implements AnomalyDetectionClient {
    private final Client client;
    private final DiscoveryNodeFilterer nodeFilterer;

    public AnomalyDetectionNodeClient(Client client, ClusterService clusterService) {
        this.client = client;
        this.nodeFilterer = new DiscoveryNodeFilterer(clusterService);
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
    public void getDetectorProfile(String detectorId, ActionListener<ADTaskProfileResponse> listener) {
        final DiscoveryNode[] eligibleNodes = this.nodeFilterer.getEligibleDataNodes();
        ADTaskProfileRequest profileRequest = new ADTaskProfileRequest(detectorId, eligibleNodes);
        this.client.execute(ADTaskProfileAction.INSTANCE, profileRequest, getADTaskProfileResponseActionListener(listener));
    }

    // We need to wrap AD-specific response type listeners around an internal listener, and re-generate the response from a generic
    // ActionResponse. This is needed to prevent classloader issues and ClassCastExceptions when executed by other plugins.
    private ActionListener<ADTaskProfileResponse> getADTaskProfileResponseActionListener(ActionListener<ADTaskProfileResponse> listener) {
        ActionListener<ADTaskProfileResponse> internalListener = ActionListener
            .wrap(profileResponse -> { listener.onResponse(profileResponse); }, listener::onFailure);
        ActionListener<ADTaskProfileResponse> actionListener = wrapActionListener(internalListener, actionResponse -> {
            ADTaskProfileResponse response = ADTaskProfileResponse.fromActionResponse(actionResponse);
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
