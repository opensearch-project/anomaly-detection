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
/* @anomaly-detection Commented until ADStatsNodesTransportAction is integrated with the SDK
package org.opensearch.ad.transport;


public class ADStatsNodesTransportActionTests extends OpenSearchIntegTestCase {

    private ADStatsNodesTransportAction action;
    private ADStats adStats;
    private Map<String, ADStat<?>> statsMap;
    private String clusterStatName1, clusterStatName2;
    private String nodeStatName1, nodeStatName2;
    private ADTaskManager adTaskManager;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        Client client = client();
        Clock clock = mock(Clock.class);
        Throttler throttler = new Throttler(clock);
        IndexNameExpressionResolver indexNameResolver = mock(IndexNameExpressionResolver.class);
        IndexUtils indexUtils = new IndexUtils(
            client,
            new ClientUtil(Settings.EMPTY, client, throttler),
            clusterService(),
            indexNameResolver
        );
        ModelManager modelManager = mock(ModelManager.class);
        CacheProvider cacheProvider = mock(CacheProvider.class);
        EntityCache cache = mock(EntityCache.class);
        when(cacheProvider.get()).thenReturn(cache);

        clusterStatName1 = "clusterStat1";
        clusterStatName2 = "clusterStat2";
        nodeStatName1 = "nodeStat1";
        nodeStatName2 = "nodeStat2";

        Settings settings = Settings.builder().put(MAX_MODEL_SIZE_PER_NODE.getKey(), 10).build();
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(MAX_MODEL_SIZE_PER_NODE)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        statsMap = new HashMap<String, ADStat<?>>() {
            {
                put(nodeStatName1, new ADStat<>(false, new CounterSupplier()));
                put(nodeStatName2, new ADStat<>(false, new ModelsOnNodeSupplier(modelManager, cacheProvider, settings, clusterService)));
                put(clusterStatName1, new ADStat<>(true, new IndexStatusSupplier(indexUtils, "index1")));
                put(clusterStatName2, new ADStat<>(true, new IndexStatusSupplier(indexUtils, "index2")));
                put(InternalStatNames.JVM_HEAP_USAGE.getName(), new ADStat<>(true, new SettableSupplier()));
            }
        };

        adStats = new ADStats(statsMap);
        JvmService jvmService = mock(JvmService.class);
        JvmStats jvmStats = mock(JvmStats.class);
        JvmStats.Mem mem = mock(JvmStats.Mem.class);

        when(jvmService.stats()).thenReturn(jvmStats);
        when(jvmStats.getMem()).thenReturn(mem);
        when(mem.getHeapUsedPercent()).thenReturn(randomShort());

        adTaskManager = mock(ADTaskManager.class);
        action = new ADStatsNodesTransportAction(
            client().threadPool(),
            clusterService(),
            mock(TransportService.class),
            mock(ActionFilters.class),
            adStats,
            jvmService,
            adTaskManager
        );
    }

    @Test
    public void testNewNodeRequest() {
        String nodeId = "nodeId1";
        ADStatsRequest adStatsRequest = new ADStatsRequest(nodeId);

        ADStatsNodeRequest adStatsNodeRequest1 = new ADStatsNodeRequest(adStatsRequest);
        ADStatsNodeRequest adStatsNodeRequest2 = action.newNodeRequest(adStatsRequest);

        assertEquals(adStatsNodeRequest1.getADStatsRequest(), adStatsNodeRequest2.getADStatsRequest());
    }

    @Test
    public void testNodeOperation() {
        String nodeId = clusterService().localNode().getId();
        ADStatsRequest adStatsRequest = new ADStatsRequest((nodeId));
        adStatsRequest.clear();

        Set<String> statsToBeRetrieved = new HashSet<>(Arrays.asList(nodeStatName1, nodeStatName2));

        for (String stat : statsToBeRetrieved) {
            adStatsRequest.addStat(stat);
        }

        ADStatsNodeResponse response = action.nodeOperation(new ADStatsNodeRequest(adStatsRequest));

        Map<String, Object> stats = response.getStatsMap();

        assertEquals(statsToBeRetrieved.size(), stats.size());
        for (String statName : stats.keySet()) {
            assertTrue(statsToBeRetrieved.contains(statName));
        }
    }

    @Test
    public void testNodeOperationWithJvmHeapUsage() {
        String nodeId = clusterService().localNode().getId();
        ADStatsRequest adStatsRequest = new ADStatsRequest((nodeId));
        adStatsRequest.clear();

        Set<String> statsToBeRetrieved = new HashSet<>(Arrays.asList(nodeStatName1, InternalStatNames.JVM_HEAP_USAGE.getName()));

        for (String stat : statsToBeRetrieved) {
            adStatsRequest.addStat(stat);
        }

        ADStatsNodeResponse response = action.nodeOperation(new ADStatsNodeRequest(adStatsRequest));

        Map<String, Object> stats = response.getStatsMap();

        assertEquals(statsToBeRetrieved.size(), stats.size());
        for (String statName : stats.keySet()) {
            assertTrue(statsToBeRetrieved.contains(statName));
        }
    }
}
*/
