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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.timeseries.constant.CommonName;

/**
 * Util class to filter unwanted node types
 *
 */
public class DiscoveryNodeFilterer {
    private static final Logger LOG = LogManager.getLogger(DiscoveryNodeFilterer.class);
    private final ClusterService clusterService;
    private final HotDataNodePredicate eligibleNodeFilter;

    public DiscoveryNodeFilterer(ClusterService clusterService) {
        this.clusterService = clusterService;
        eligibleNodeFilter = new HotDataNodePredicate();
    }

    /**
     * Find nodes that are elibile to be used by us.  For example, Ultrawarm
     *  introduces warm nodes into the ES cluster. Currently, we distribute
     *  model partitions to all data nodes in the cluster randomly, which
     *  could cause a model performance downgrade issue once warm nodes
     *  are throttled due to resource limitations. The PR excludes warm nodes
     *  to place model partitions.
     * @return an array of eligible data nodes
     */
    public DiscoveryNode[] getEligibleDataNodes() {
        ClusterState state = this.clusterService.state();
        final List<DiscoveryNode> eligibleNodes = new ArrayList<>();
        for (DiscoveryNode node : state.nodes()) {
            if (eligibleNodeFilter.test(node)) {
                eligibleNodes.add(node);
            }
        }
        return eligibleNodes.toArray(new DiscoveryNode[0]);
    }

    public DiscoveryNode[] getAllNodes() {
        ClusterState state = this.clusterService.state();
        final List<DiscoveryNode> nodes = new ArrayList<>();
        for (DiscoveryNode node : state.nodes()) {
            nodes.add(node);
        }
        return nodes.toArray(new DiscoveryNode[0]);
    }

    public boolean isEligibleDataNode(DiscoveryNode node) {
        return eligibleNodeFilter.test(node);
    }

    /**
     *
     * @return the number of eligible data nodes
     */
    public int getNumberOfEligibleDataNodes() {
        return getEligibleDataNodes().length;
    }

    /**
     * @param node a discovery node
     * @return whether we should use this node for AD
     */
    public boolean isEligibleNode(DiscoveryNode node) {
        return eligibleNodeFilter.test(node);
    }

    static class HotDataNodePredicate implements Predicate<DiscoveryNode> {
        @Override
        public boolean test(DiscoveryNode discoveryNode) {
            return discoveryNode.isDataNode()
                && discoveryNode
                    .getAttributes()
                    .getOrDefault(CommonName.BOX_TYPE_KEY, CommonName.HOT_BOX_TYPE)
                    .equals(CommonName.HOT_BOX_TYPE);
        }
    }
}
