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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.cluster.node.DiscoveryNodeRole.BUILT_IN_ROLES;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.opensearch.Version;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

public class HashRingTests extends AbstractADTest {

    private ClusterService clusterService;
    private DiscoveryNodeFilterer nodeFilter;
    private Settings settings;
    private Clock clock;
    private Client client;
    private ADDataMigrator dataMigrator;

    private DiscoveryNode createNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), attributes, BUILT_IN_ROLES, Version.CURRENT);
    }

    private void setNodeState() {
        setNodeState(emptyMap());
    }

    private void setNodeState(Map<String, String> attributesForNode1) {
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        List<DiscoveryNode> discoveryNodes = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            DiscoveryNode node = null;
            if (i != 1) {
                node = createNode(Integer.toString(i), emptyMap());
            } else {
                node = createNode(Integer.toString(i), attributesForNode1);
            }

            discoBuilder = discoBuilder.add(node);
            discoveryNodes.add(node);
        }
        discoBuilder.localNodeId("1");
        discoBuilder.masterNodeId("0");
        ClusterState.Builder stateBuilder = ClusterState.builder(clusterService.getClusterName());
        stateBuilder.nodes(discoBuilder);
        ClusterState clusterState = stateBuilder.build();
        setState(clusterService.getClusterApplierService(), clusterState);
    }

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(HashRingTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(HashRing.class);
        clusterService = createClusterService(threadPool);
        HashMap<String, String> ignoredAttributes = new HashMap<String, String>();
        ignoredAttributes.put(CommonName.BOX_TYPE_KEY, CommonName.WARM_BOX_TYPE);
        nodeFilter = new DiscoveryNodeFilterer(clusterService);
        client = mock(Client.class);
        dataMigrator = mock(ADDataMigrator.class);

        settings = Settings
            .builder()
            .put("plugins.anomaly_detection.cluster_state_change_cooldown_minutes", TimeValue.timeValueMinutes(5))
            .build();
        clock = mock(Clock.class);
        when(clock.millis()).thenReturn(700000L);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
        clusterService.close();
    }

    @Ignore
    public void testGetOwningNode() {
        setNodeState();

        HashRing ring = new HashRing(nodeFilter, clock, settings, client, clusterService, dataMigrator);
        Optional<DiscoveryNode> node = ring.getOwningNodeWithSameLocalAdVersionForRealtimeJob("http-latency-rcf-1");
        assertTrue(node.isPresent());
        String id = node.get().getId();
        assertTrue(id.equals("1") || id.equals("2"));

        when(clock.millis()).thenReturn(700001L);
        Optional<DiscoveryNode> node2 = ring.getOwningNodeWithSameLocalAdVersionForRealtimeJob("http-latency-rcf-1");
        assertEquals(node, node2);
        assertTrue(testAppender.containsMessage(HashRing.COOLDOWN_MSG));
    }

    @Ignore
    public void testWarmNodeExcluded() {
        HashMap<String, String> attributesForNode1 = new HashMap<>();
        attributesForNode1.put(CommonName.BOX_TYPE_KEY, CommonName.WARM_BOX_TYPE);
        setNodeState(attributesForNode1);

        HashRing ring = new HashRing(nodeFilter, clock, settings, client, clusterService, dataMigrator);
        Optional<DiscoveryNode> node = ring.getOwningNodeWithSameLocalAdVersionForRealtimeJob("http-latency-rcf-1");
        assertTrue(node.isPresent());
        String id = node.get().getId();
        assertTrue(id.equals("2"));
    }
}
