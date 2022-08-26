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

package org.opensearch.ad.cluster;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.opensearch.cluster.node.DiscoveryNodeRole.BUILT_IN_ROLES;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;

import java.util.HashMap;
import java.util.Optional;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.gateway.GatewayService;

public class ADClusterEventListenerTests extends AbstractADTest {
    private final String clusterManagerNodeId = "clusterManagerNode";
    private final String dataNode1Id = "dataNode1";
    private final String clusterName = "multi-node-cluster";

    private ClusterService clusterService;
    private ADClusterEventListener listener;
    private HashRing hashRing;
    private ClusterState oldClusterState;
    private ClusterState newClusterState;
    private DiscoveryNode clusterManagerNode;
    private DiscoveryNode dataNode1;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(ADClusterEventListenerTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(ADClusterEventListener.class);
        clusterService = createClusterService(threadPool);
        hashRing = mock(HashRing.class);

        clusterManagerNode = new DiscoveryNode(
            clusterManagerNodeId,
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        dataNode1 = new DiscoveryNode(dataNode1Id, buildNewFakeTransportAddress(), emptyMap(), BUILT_IN_ROLES, Version.CURRENT);
        oldClusterState = ClusterState
            .builder(new ClusterName(clusterName))
            .nodes(
                new DiscoveryNodes.Builder().clusterManagerNodeId(clusterManagerNodeId).localNodeId(clusterManagerNodeId).add(clusterManagerNode)
            )
            .build();
        newClusterState = ClusterState
            .builder(new ClusterName(clusterName))
            .nodes(
                new DiscoveryNodes.Builder()
                    .clusterManagerNodeId(clusterManagerNodeId)
                    .localNodeId(dataNode1Id)
                    .add(clusterManagerNode)
                    .add(dataNode1)
            )
            .build();

        listener = new ADClusterEventListener(clusterService, hashRing);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
        clusterService = null;
        hashRing = null;
        oldClusterState = null;
        listener = null;
    }

    public void testUnchangedClusterState() {
        listener.clusterChanged(new ClusterChangedEvent("foo", oldClusterState, oldClusterState));
        assertTrue(!testAppender.containsMessage(ADClusterEventListener.NODE_CHANGED_MSG));
    }

    public void testIsWarmNode() {
        HashMap<String, String> attributesForNode1 = new HashMap<>();
        attributesForNode1.put(CommonName.BOX_TYPE_KEY, CommonName.WARM_BOX_TYPE);
        dataNode1 = new DiscoveryNode(dataNode1Id, buildNewFakeTransportAddress(), attributesForNode1, BUILT_IN_ROLES, Version.CURRENT);

        ClusterState warmNodeClusterState = ClusterState
            .builder(new ClusterName(clusterName))
            .nodes(
                new DiscoveryNodes.Builder()
                    .clusterManagerNodeId(clusterManagerNodeId)
                    .localNodeId(dataNode1Id)
                    .add(clusterManagerNode)
                    .add(dataNode1)
            )
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
            .build();
        listener.clusterChanged(new ClusterChangedEvent("foo", warmNodeClusterState, oldClusterState));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.NOT_RECOVERED_MSG));
    }

    public void testNotRecovered() {
        ClusterState blockedClusterState = ClusterState
            .builder(new ClusterName(clusterName))
            .nodes(
                new DiscoveryNodes.Builder()
                    .clusterManagerNodeId(clusterManagerNodeId)
                    .localNodeId(dataNode1Id)
                    .add(clusterManagerNode)
                    .add(dataNode1)
            )
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
            .build();
        listener.clusterChanged(new ClusterChangedEvent("foo", blockedClusterState, oldClusterState));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.NOT_RECOVERED_MSG));
    }

    class ListenerRunnable implements Runnable {

        @Override
        public void run() {
            listener.clusterChanged(new ClusterChangedEvent("foo", newClusterState, oldClusterState));
        }
    }

    public void testInProgress() {
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            Thread.sleep(1000);
            listener.onResponse(true);
            return null;
        }).when(hashRing).buildCircles(any(), any());
        new Thread(new ListenerRunnable()).start();
        listener.clusterChanged(new ClusterChangedEvent("bar", newClusterState, oldClusterState));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.IN_PROGRESS_MSG));
    }

    public void testNodeAdded() {
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(hashRing).buildCircles(any(), any());

        doAnswer(invocation -> Optional.of(clusterManagerNode))
            .when(hashRing)
            .getOwningNodeWithSameLocalAdVersionForRealtimeAD(any(String.class));

        listener.clusterChanged(new ClusterChangedEvent("foo", newClusterState, oldClusterState));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.NODE_CHANGED_MSG));
        assertTrue(testAppender.containsMessage("node removed: false, node added: true"));
    }

    public void testNodeRemoved() {
        ClusterState twoDataNodeClusterState = ClusterState
            .builder(new ClusterName(clusterName))
            .nodes(
                new DiscoveryNodes.Builder()
                    .clusterManagerNodeId(clusterManagerNodeId)
                    .localNodeId(dataNode1Id)
                    .add(new DiscoveryNode(clusterManagerNodeId, buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT))
                    .add(dataNode1)
                    .add(new DiscoveryNode("dataNode2", buildNewFakeTransportAddress(), emptyMap(), BUILT_IN_ROLES, Version.CURRENT))
            )
            .build();

        listener.clusterChanged(new ClusterChangedEvent("foo", newClusterState, twoDataNodeClusterState));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.NODE_CHANGED_MSG));
        assertTrue(testAppender.containsMessage("node removed: true, node added: true"));
    }
}
