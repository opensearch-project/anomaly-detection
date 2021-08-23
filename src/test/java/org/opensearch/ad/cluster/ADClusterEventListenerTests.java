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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
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
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.gateway.GatewayService;

public class ADClusterEventListenerTests extends AbstractADTest {
    private final String masterNodeId = "masterNode";
    private final String dataNode1Id = "dataNode1";
    private final String clusterName = "multi-node-cluster";

    private ClusterService clusterService;
    private ADClusterEventListener listener;
    private HashRing hashRing;
    private ModelManager modelManager;
    private ClusterState oldClusterState;
    private ClusterState newClusterState;
    private DiscoveryNode masterNode;
    private DiscoveryNode dataNode1;
    private DiscoveryNodeFilterer nodeFilter;

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
        modelManager = mock(ModelManager.class);

        nodeFilter = new DiscoveryNodeFilterer(clusterService);
        masterNode = new DiscoveryNode(masterNodeId, buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        dataNode1 = new DiscoveryNode(dataNode1Id, buildNewFakeTransportAddress(), emptyMap(), BUILT_IN_ROLES, Version.CURRENT);
        oldClusterState = ClusterState
            .builder(new ClusterName(clusterName))
            .nodes(new DiscoveryNodes.Builder().masterNodeId(masterNodeId).localNodeId(masterNodeId).add(masterNode))
            .build();
        newClusterState = ClusterState
            .builder(new ClusterName(clusterName))
            .nodes(new DiscoveryNodes.Builder().masterNodeId(masterNodeId).localNodeId(dataNode1Id).add(masterNode).add(dataNode1))
            .build();

        listener = new ADClusterEventListener(clusterService, hashRing, modelManager, nodeFilter);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
        clusterService = null;
        hashRing = null;
        modelManager = null;
        oldClusterState = null;
        listener = null;
    }

    public void testIsMasterNode() {
        listener.clusterChanged(new ClusterChangedEvent("foo", oldClusterState, oldClusterState));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.NODE_NOT_APPLIED_MSG));
    }

    public void testIsWarmNode() {
        HashMap<String, String> attributesForNode1 = new HashMap<>();
        attributesForNode1.put(CommonName.BOX_TYPE_KEY, CommonName.WARM_BOX_TYPE);
        dataNode1 = new DiscoveryNode(dataNode1Id, buildNewFakeTransportAddress(), attributesForNode1, BUILT_IN_ROLES, Version.CURRENT);

        ClusterState warmNodeClusterState = ClusterState
            .builder(new ClusterName(clusterName))
            .nodes(new DiscoveryNodes.Builder().masterNodeId(masterNodeId).localNodeId(dataNode1Id).add(masterNode).add(dataNode1))
            .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
            .build();
        listener.clusterChanged(new ClusterChangedEvent("foo", warmNodeClusterState, oldClusterState));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.NODE_NOT_APPLIED_MSG));
    }

    public void testNotRecovered() {
        ClusterState blockedClusterState = ClusterState
            .builder(new ClusterName(clusterName))
            .nodes(new DiscoveryNodes.Builder().masterNodeId(masterNodeId).localNodeId(dataNode1Id).add(masterNode).add(dataNode1))
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

    public void testInprogress() throws InterruptedException {
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        final CountDownLatch executionLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(hashRing).buildCirclesOnAdVersions(any(), any());
        doAnswer(invocation -> {
            executionLatch.countDown();
            inProgressLatch.await();
            return emptySet();
        }).when(modelManager).getAllModelIds();
        new Thread(new ListenerRunnable()).start();
        executionLatch.await();
        listener.clusterChanged(new ClusterChangedEvent("bar", newClusterState, oldClusterState));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.IN_PROGRESS_MSG));
        inProgressLatch.countDown();
    }

    public void testNodeAdded() {
        String modelId = "123-threshold";
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(1);
            listener.onResponse(true);
            return null;
        }).when(hashRing).buildCirclesOnAdVersions(any(), any());
        doAnswer(invocation -> {
            Set<String> res = new HashSet<>();
            res.add(modelId);
            return res;
        }).when(modelManager).getAllModelIds();

        doAnswer(invocation -> Optional.of(masterNode)).when(hashRing).getOwningNodeWithSameLocalAdVersionDirectly(any(String.class));

        listener.clusterChanged(new ClusterChangedEvent("foo", newClusterState, oldClusterState));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.NODE_ADDED_MSG));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.REMOVE_MODEL_MSG + " " + modelId));
    }

    public void testNodeRemoved() {
        ClusterState twoDataNodeClusterState = ClusterState
            .builder(new ClusterName(clusterName))
            .nodes(
                new DiscoveryNodes.Builder()
                    .masterNodeId(masterNodeId)
                    .localNodeId(dataNode1Id)
                    .add(new DiscoveryNode(masterNodeId, buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT))
                    .add(dataNode1)
                    .add(new DiscoveryNode("dataNode2", buildNewFakeTransportAddress(), emptyMap(), BUILT_IN_ROLES, Version.CURRENT))
            )
            .build();

        listener.clusterChanged(new ClusterChangedEvent("foo", newClusterState, twoDataNodeClusterState));
        assertTrue(!testAppender.containsMessage(ADClusterEventListener.NODE_ADDED_MSG));
        assertTrue(testAppender.containsMessage(ADClusterEventListener.NODE_REMOVED_MSG));
    }
}
