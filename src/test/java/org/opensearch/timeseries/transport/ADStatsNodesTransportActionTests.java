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

package org.opensearch.timeseries.transport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MAX_MODEL_SIZE_PER_NODE;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.stats.suppliers.ADModelsOnNodeSupplier;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.ADStatsNodesTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.stats.InternalStatNames;
import org.opensearch.timeseries.stats.TimeSeriesStat;
import org.opensearch.timeseries.stats.suppliers.CounterSupplier;
import org.opensearch.timeseries.stats.suppliers.IndexStatusSupplier;
import org.opensearch.timeseries.stats.suppliers.SettableSupplier;
import org.opensearch.timeseries.util.IndexUtils;
import org.opensearch.transport.TransportService;

public class ADStatsNodesTransportActionTests extends OpenSearchIntegTestCase {

    private ADStatsNodesTransportAction action;
    private ADStats adStats;
    private Map<String, TimeSeriesStat<?>> statsMap;
    private String clusterStatName1, clusterStatName2;
    private String nodeStatName1, nodeStatName2;
    private ADTaskManager adTaskManager;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        Client client = client();
        Clock clock = mock(Clock.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        IndexNameExpressionResolver indexNameResolver = mock(IndexNameExpressionResolver.class);
        IndexUtils indexUtils = new IndexUtils(clusterService(), indexNameResolver);
        ADModelManager modelManager = mock(ADModelManager.class);
        ADCacheProvider cacheProvider = mock(ADCacheProvider.class);
        ADPriorityCache cache = mock(ADPriorityCache.class);
        when(cacheProvider.get()).thenReturn(cache);

        clusterStatName1 = "clusterStat1";
        clusterStatName2 = "clusterStat2";
        nodeStatName1 = "nodeStat1";
        nodeStatName2 = "nodeStat2";

        Settings settings = Settings.builder().put(AD_MAX_MODEL_SIZE_PER_NODE.getKey(), 10).build();
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AD_MAX_MODEL_SIZE_PER_NODE)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        statsMap = new HashMap<String, TimeSeriesStat<?>>() {
            {
                put(nodeStatName1, new TimeSeriesStat<>(false, new CounterSupplier()));
                put(
                    nodeStatName2,
                    new TimeSeriesStat<>(false, new ADModelsOnNodeSupplier(modelManager, cacheProvider, settings, clusterService))
                );
                put(clusterStatName1, new TimeSeriesStat<>(true, new IndexStatusSupplier(indexUtils, "index1")));
                put(clusterStatName2, new TimeSeriesStat<>(true, new IndexStatusSupplier(indexUtils, "index2")));
                put(InternalStatNames.JVM_HEAP_USAGE.getName(), new TimeSeriesStat<>(true, new SettableSupplier()));
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
        StatsRequest adStatsRequest = new StatsRequest(nodeId);

        StatsNodeRequest adStatsNodeRequest1 = new StatsNodeRequest(adStatsRequest);
        StatsNodeRequest adStatsNodeRequest2 = action.newNodeRequest(adStatsRequest);

        assertEquals(adStatsNodeRequest1.getADStatsRequest(), adStatsNodeRequest2.getADStatsRequest());
    }

    @Test
    public void testNodeOperation() {
        String nodeId = clusterService().localNode().getId();
        StatsRequest adStatsRequest = new StatsRequest((nodeId));
        adStatsRequest.clear();

        Set<String> statsToBeRetrieved = new HashSet<>(Arrays.asList(nodeStatName1, nodeStatName2));

        for (String stat : statsToBeRetrieved) {
            adStatsRequest.addStat(stat);
        }

        StatsNodeResponse response = action.nodeOperation(new StatsNodeRequest(adStatsRequest));

        Map<String, Object> stats = response.getStatsMap();

        assertEquals(statsToBeRetrieved.size(), stats.size());
        for (String statName : stats.keySet()) {
            assertTrue(statsToBeRetrieved.contains(statName));
        }
    }

    @Test
    public void testNodeOperationWithJvmHeapUsage() {
        String nodeId = clusterService().localNode().getId();
        StatsRequest adStatsRequest = new StatsRequest((nodeId));
        adStatsRequest.clear();

        Set<String> statsToBeRetrieved = new HashSet<>(Arrays.asList(nodeStatName1, InternalStatNames.JVM_HEAP_USAGE.getName()));

        for (String stat : statsToBeRetrieved) {
            adStatsRequest.addStat(stat);
        }

        StatsNodeResponse response = action.nodeOperation(new StatsNodeRequest(adStatsRequest));

        Map<String, Object> stats = response.getStatsMap();

        assertEquals(statsToBeRetrieved.size(), stats.size());
        for (String statName : stats.keySet()) {
            assertTrue(statsToBeRetrieved.contains(statName));
        }
    }
}
