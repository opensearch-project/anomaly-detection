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

package com.amazon.opendistroforelasticsearch.ad.transport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.settings.Settings;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.caching.CacheProvider;
import com.amazon.opendistroforelasticsearch.ad.caching.EntityCache;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStat;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.stats.InternalStatNames;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.CounterSupplier;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.IndexStatusSupplier;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.ModelsOnNodeSupplier;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.SettableSupplier;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import com.amazon.opendistroforelasticsearch.ad.util.Throttler;

public class ADStatsNodesTransportActionTests extends OpenSearchIntegTestCase {

    private ADStatsNodesTransportAction action;
    private ADStats adStats;
    private Map<String, ADStat<?>> statsMap;
    private String clusterStatName1, clusterStatName2;
    private String nodeStatName1, nodeStatName2;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        Client client = client();
        Clock clock = mock(Clock.class);
        Throttler throttler = new Throttler(clock);
        ThreadPool threadPool = mock(ThreadPool.class);
        IndexNameExpressionResolver indexNameResolver = mock(IndexNameExpressionResolver.class);
        IndexUtils indexUtils = new IndexUtils(
            client,
            new ClientUtil(Settings.EMPTY, client, throttler, threadPool),
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

        statsMap = new HashMap<String, ADStat<?>>() {
            {
                put(nodeStatName1, new ADStat<>(false, new CounterSupplier()));
                put(nodeStatName2, new ADStat<>(false, new ModelsOnNodeSupplier(modelManager, cacheProvider)));
                put(clusterStatName1, new ADStat<>(true, new IndexStatusSupplier(indexUtils, "index1")));
                put(clusterStatName2, new ADStat<>(true, new IndexStatusSupplier(indexUtils, "index2")));
                put(InternalStatNames.JVM_HEAP_USAGE.getName(), new ADStat<>(true, new SettableSupplier()));
            }
        };

        adStats = new ADStats(indexUtils, modelManager, statsMap);
        JvmService jvmService = mock(JvmService.class);
        JvmStats jvmStats = mock(JvmStats.class);
        JvmStats.Mem mem = mock(JvmStats.Mem.class);

        when(jvmService.stats()).thenReturn(jvmStats);
        when(jvmStats.getMem()).thenReturn(mem);
        when(mem.getHeapUsedPercent()).thenReturn(randomShort());

        action = new ADStatsNodesTransportAction(
            client().threadPool(),
            clusterService(),
            mock(TransportService.class),
            mock(ActionFilters.class),
            adStats,
            jvmService
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
