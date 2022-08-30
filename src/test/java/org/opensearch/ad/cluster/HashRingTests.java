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
package org.opensearch.ad.cluster;


public class HashRingTests extends ADUnitTestCase {

    private ClusterService clusterService;
    private DiscoveryNodeFilterer nodeFilter;
    private Settings settings;
    private Clock clock;
    private Client client;
    private ClusterAdminClient clusterAdminClient;
    private AdminClient adminClient;
    private ADDataMigrator dataMigrator;
    private HashRing hashRing;
    private DiscoveryNodes.Delta delta;
    private String localNodeId;
    private String newNodeId;
    private String warmNodeId;
    private DiscoveryNode localNode;
    private DiscoveryNode newNode;
    private DiscoveryNode warmNode;
    private ModelManager modelManager;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        localNodeId = "localNode";
        localNode = createNode(localNodeId, "127.0.0.1", 9200, emptyMap());
        newNodeId = "newNode";
        newNode = createNode(newNodeId, "127.0.0.2", 9201, emptyMap());
        warmNodeId = "warmNode";
        warmNode = createNode(warmNodeId, "127.0.0.3", 9202, ImmutableMap.of(CommonName.BOX_TYPE_KEY, CommonName.WARM_BOX_TYPE));

        settings = Settings.builder().put(COOLDOWN_MINUTES.getKey(), TimeValue.timeValueSeconds(5)).build();
        ClusterSettings clusterSettings = clusterSetting(settings, COOLDOWN_MINUTES);
        clusterService = spy(new ClusterService(settings, clusterSettings, null));

        nodeFilter = spy(new DiscoveryNodeFilterer(clusterService));
        client = mock(Client.class);
        dataMigrator = mock(ADDataMigrator.class);

        clock = mock(Clock.class);
        when(clock.millis()).thenReturn(700000L);

        delta = mock(DiscoveryNodes.Delta.class);

        adminClient = mock(AdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        clusterAdminClient = mock(ClusterAdminClient.class);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);

        String modelId = "123_model_threshold";
        modelManager = mock(ModelManager.class);
        doAnswer(invocation -> {
            Set<String> res = new HashSet<>();
            res.add(modelId);
            return res;
        }).when(modelManager).getAllModelIds();

        hashRing = spy(new HashRing(nodeFilter, clock, settings, client, clusterService, dataMigrator, modelManager));
    }

    public void testGetOwningNodeWithEmptyResult() throws UnknownHostException {
        DiscoveryNode node1 = createNode(Integer.toString(1), "127.0.0.4", 9204, emptyMap());
        doReturn(node1).when(clusterService).localNode();

        Optional<DiscoveryNode> node = hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD("http-latency-rcf-1");
        assertFalse(node.isPresent());
    }

    public void testGetOwningNode() throws UnknownHostException {
        List<DiscoveryNode> addedNodes = setupNodeDelta();

        // Add first node,
        hashRing.buildCircles(delta, ActionListener.wrap(r -> {
            Optional<DiscoveryNode> node = hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD("http-latency-rcf-1");
            assertTrue(node.isPresent());
            assertTrue(asList(newNodeId, localNodeId).contains(node.get().getId()));
            DiscoveryNode[] nodesWithSameLocalAdVersion = hashRing.getNodesWithSameLocalAdVersion();
            Set<String> nodesWithSameLocalAdVersionIds = new HashSet<>();
            for (DiscoveryNode n : nodesWithSameLocalAdVersion) {
                nodesWithSameLocalAdVersionIds.add(n.getId());
            }
            assertFalse("Should not build warm node into hash ring", nodesWithSameLocalAdVersionIds.contains(warmNodeId));
            assertEquals("Wrong hash ring size", 2, nodesWithSameLocalAdVersion.length);
            assertEquals(
                "Wrong hash ring size for historical analysis",
                2,
                hashRing.getNodesWithSameAdVersion(Version.V_1_1_0, false).size()
            );
            // Circles for realtime AD will change as it's eligible to build for when its empty
            assertEquals("Wrong hash ring size for realtime AD", 2, hashRing.getNodesWithSameAdVersion(Version.V_1_1_0, true).size());
        }, e -> {
            logger.error("building hash ring failed", e);
            assertFalse("Build hash ring failed", true);
        }));

        // Second new node joins cluster, test realtime circles will not update.
        String newNodeId2 = "newNode2";
        DiscoveryNode newNode2 = createNode(newNodeId2, "127.0.0.4", 9200, emptyMap());
        addedNodes.add(newNode2);
        when(delta.addedNodes()).thenReturn(addedNodes);
        setupClusterAdminClient(localNode, newNode, newNode2);
        hashRing.buildCircles(delta, ActionListener.wrap(r -> {
            assertEquals(
                "Wrong hash ring size for historical analysis",
                3,
                hashRing.getNodesWithSameAdVersion(Version.V_1_1_0, false).size()
            );
            // Circles for realtime AD will not change as it's eligible to rebuild
            assertEquals("Wrong hash ring size for realtime AD", 2, hashRing.getNodesWithSameAdVersion(Version.V_1_1_0, true).size());
        }, e -> {
            logger.error("building hash ring failed", e);

            assertFalse("Build hash ring failed", true);
        }));

        // Mock it's eligible to rebuild circles for realtime AD, then add new node. Realtime circles should change.
        when(hashRing.eligibleToRebuildCirclesForRealtimeAD()).thenReturn(true);
        String newNodeId3 = "newNode3";
        DiscoveryNode newNode3 = createNode(newNodeId3, "127.0.0.5", 9200, emptyMap());
        addedNodes.add(newNode3);
        when(delta.addedNodes()).thenReturn(addedNodes);
        setupClusterAdminClient(localNode, newNode, newNode2, newNode3);
        hashRing.buildCircles(delta, ActionListener.wrap(r -> {
            assertEquals(
                "Wrong hash ring size for historical analysis",
                4,
                hashRing.getNodesWithSameAdVersion(Version.V_1_1_0, false).size()
            );
            assertEquals("Wrong hash ring size for realtime AD", 4, hashRing.getNodesWithSameAdVersion(Version.V_1_1_0, true).size());
        }, e -> {
            logger.error("building hash ring failed", e);
            assertFalse("Failed to build hash ring", true);
        }));
    }

    public void testGetAllEligibleDataNodesWithKnownAdVersionAndGetNodeByAddress() {
        setupNodeDelta();
        hashRing.getAllEligibleDataNodesWithKnownAdVersion(nodes -> {
            assertEquals("Wrong hash ring size for historical analysis", 2, nodes.length);
            Optional<DiscoveryNode> node = hashRing.getNodeByAddress(newNode.getAddress());
            assertTrue(node.isPresent());
            assertEquals(newNodeId, node.get().getId());
        }, ActionListener.wrap(r -> {}, e -> { assertFalse("Failed to build hash ring", true); }));
    }

    public void testBuildAndGetOwningNodeWithSameLocalAdVersion() {
        setupNodeDelta();
        hashRing
            .buildAndGetOwningNodeWithSameLocalAdVersion(
                "testModelId",
                node -> { assertTrue(node.isPresent()); },
                ActionListener.wrap(r -> {}, e -> { assertFalse("Failed to build hash ring", true); })
            );
    }

    private List<DiscoveryNode> setupNodeDelta() {
        List<DiscoveryNode> addedNodes = new ArrayList<>();
        addedNodes.add(newNode);

        List<DiscoveryNode> removedNodes = asList();

        when(delta.removed()).thenReturn(false);
        when(delta.added()).thenReturn(true);
        when(delta.removedNodes()).thenReturn(removedNodes);
        when(delta.addedNodes()).thenReturn(addedNodes);

        doReturn(localNode).when(clusterService).localNode();
        setupClusterAdminClient(localNode, newNode, warmNode);

        doReturn(new DiscoveryNode[] { localNode, newNode }).when(nodeFilter).getEligibleDataNodes();
        doReturn(new DiscoveryNode[] { localNode, newNode, warmNode }).when(nodeFilter).getAllNodes();
        return addedNodes;
    }

    private void setupClusterAdminClient(DiscoveryNode... nodes) {
        doAnswer(invocation -> {
            ActionListener<NodesInfoResponse> listener = invocation.getArgument(1);
            List<NodeInfo> nodeInfos = new ArrayList<>();
            for (DiscoveryNode node : nodes) {
                nodeInfos.add(createNodeInfo(node, "1.1.0.0"));
            }
            NodesInfoResponse nodesInfoResponse = new NodesInfoResponse(ClusterName.DEFAULT, nodeInfos, ImmutableList.of());
            listener.onResponse(nodesInfoResponse);
            return null;
        }).when(clusterAdminClient).nodesInfo(any(), any());
    }

    private NodeInfo createNodeInfo(DiscoveryNode node, String version) {
        List<PluginInfo> plugins = new ArrayList<>();
        plugins
            .add(
                new PluginInfo(
                    CommonName.AD_PLUGIN_NAME,
                    randomAlphaOfLengthBetween(3, 10),
                    version,
                    Version.CURRENT,
                    "1.8",
                    randomAlphaOfLengthBetween(3, 10),
                    randomAlphaOfLengthBetween(3, 10),
                    ImmutableList.of(),
                    randomBoolean()
                )
            );
        List<PluginInfo> modules = new ArrayList<>();
        modules.addAll(plugins);
        PluginsAndModules pluginsAndModules = new PluginsAndModules(plugins, modules);
        return new NodeInfo(
            Version.CURRENT,
            Build.CURRENT,
            node,
            settings,
            null,
            null,
            null,
            null,
            null,
            null,
            pluginsAndModules,
            null,
            null,
            null
        );
    }
}
*/
