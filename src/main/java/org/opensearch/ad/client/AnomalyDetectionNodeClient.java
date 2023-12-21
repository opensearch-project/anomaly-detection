/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.client;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.transport.ADTaskProfileAction;
import org.opensearch.ad.transport.ADTaskProfileRequest;
import org.opensearch.ad.transport.ADTaskProfileResponse;
import org.opensearch.ad.transport.SearchAnomalyDetectorAction;
import org.opensearch.ad.transport.SearchAnomalyResultAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;

public class AnomalyDetectionNodeClient implements AnomalyDetectionClient {
    private final Client client;
    private final ClusterService clusterService;
    private final HotDataNodePredicate eligibleNodeFilter;

    public AnomalyDetectionNodeClient(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
        this.eligibleNodeFilter = new HotDataNodePredicate();
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

        // TODO: clean up
        // Logic to determine eligible nodes comes from org.opensearch.timeseries.util.DiscoveryNodeFilterer
        // There is no clean way to consume that within this client's constructor since it will be instantiated
        // outside of this plugin typically. So we re-use that logic here
        ClusterState state = this.clusterService.state();
        final List<DiscoveryNode> eligibleNodes = new ArrayList<>();
        for (DiscoveryNode node : state.nodes()) {
            if (this.eligibleNodeFilter.test(node)) {
                eligibleNodes.add(node);
            }
        }
        final DiscoveryNode[] eligibleNodesAsArray = eligibleNodes.toArray(new DiscoveryNode[0]);

        ADTaskProfileRequest profileRequest = new ADTaskProfileRequest(detectorId, eligibleNodesAsArray);
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

    static class HotDataNodePredicate implements Predicate<DiscoveryNode> {
        @Override
        public boolean test(DiscoveryNode discoveryNode) {
            return discoveryNode.isDataNode()
                && discoveryNode
                    .getAttributes()
                    .getOrDefault(ADCommonName.BOX_TYPE_KEY, ADCommonName.HOT_BOX_TYPE)
                    .equals(ADCommonName.HOT_BOX_TYPE);
        }
    }
}
