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

package test.org.opensearch.ad.util;

import static org.mockito.Mockito.mock;
import static org.opensearch.cluster.node.DiscoveryNodeRole.CLUSTER_MANAGER_ROLE;
import static org.opensearch.cluster.node.DiscoveryNodeRole.DATA_ROLE;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.core.common.transport.TransportAddress;

public class ClusterCreation {
    /**
     * Creates a cluster state where local node and clusterManager node can be specified
     *
     * @param localNode  node in allNodes that is the local node
     * @param clusterManagerNode node in allNodes that is the clusterManager node. Can be null if no clusterManager exists
     * @param allNodes   all nodes in the cluster
     * @return cluster state
     */
    public static ClusterState state(
        ClusterName name,
        DiscoveryNode localNode,
        DiscoveryNode clusterManagerNode,
        List<DiscoveryNode> allNodes
    ) {
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        for (DiscoveryNode node : allNodes) {
            discoBuilder.add(node);
        }
        if (clusterManagerNode != null) {
            discoBuilder.clusterManagerNodeId(clusterManagerNode.getId());
        }
        discoBuilder.localNodeId(localNode.getId());

        ClusterState.Builder state = ClusterState.builder(name);
        state.nodes(discoBuilder);
        state.metadata(Metadata.builder().generateClusterUuidIfNeeded());
        return state.build();
    }

    /**
     * Create data node map
     * @param numDataNodes the number of data nodes
     * @return data nodes map
     *
     * TODO: ModelManagerTests has the same method.  Refactor.
     */
    public static Map<String, DiscoveryNode> createDataNodes(int numDataNodes) {
        Map<String, DiscoveryNode> dataNodes = new HashMap<>();
        for (int i = 0; i < numDataNodes; i++) {
            dataNodes.put("foo" + i, mock(DiscoveryNode.class));
        }
        return Collections.unmodifiableMap(dataNodes);
    }

    /**
     * Create a cluster state with 1 clusterManager node and a few data nodes
     * @param numDataNodes the number of data nodes
     * @return the cluster state
     */
    public static ClusterState state(int numDataNodes) {
        DiscoveryNode clusterManagerNode = new DiscoveryNode(
            "foo0",
            "foo0",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            Collections.emptyMap(),
            Collections.singleton(CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        List<DiscoveryNode> allNodes = new ArrayList<>();
        allNodes.add(clusterManagerNode);
        for (int i = 1; i <= numDataNodes - 1; i++) {
            allNodes
                .add(
                    new DiscoveryNode(
                        "foo" + i,
                        "foo" + i,
                        new TransportAddress(InetAddress.getLoopbackAddress(), 9300 + i),
                        Collections.emptyMap(),
                        Collections.singleton(DATA_ROLE),
                        Version.CURRENT
                    )
                );
        }
        return state(new ClusterName("test"), clusterManagerNode, clusterManagerNode, allNodes);
    }
}
