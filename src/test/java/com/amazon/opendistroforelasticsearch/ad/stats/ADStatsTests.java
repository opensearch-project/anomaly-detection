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

package com.amazon.opendistroforelasticsearch.ad.stats;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.test.OpenSearchTestCase;

import test.com.amazon.opendistroforelasticsearch.ad.util.MLUtil;

import com.amazon.opendistroforelasticsearch.ad.caching.CacheProvider;
import com.amazon.opendistroforelasticsearch.ad.caching.EntityCache;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.HybridThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.CounterSupplier;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.IndexStatusSupplier;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.ModelsOnNodeSupplier;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import com.amazon.randomcutforest.RandomCutForest;

public class ADStatsTests extends OpenSearchTestCase {

    private Map<String, ADStat<?>> statsMap;
    private ADStats adStats;
    private RandomCutForest rcf;
    private HybridThresholdingModel thresholdingModel;
    private String clusterStatName1, clusterStatName2;
    private String nodeStatName1, nodeStatName2;

    @Mock
    private Clock clock;

    @Mock
    private ModelManager modelManager;

    @Mock
    private CacheProvider cacheProvider;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        rcf = RandomCutForest.builder().dimensions(1).sampleSize(1).numberOfTrees(1).build();
        thresholdingModel = new HybridThresholdingModel(1e-8, 1e-5, 200, 10_000, 2, 5_000_000);

        List<ModelState<?>> modelsInformation = new ArrayList<>(
            Arrays
                .asList(
                    new ModelState<>(rcf, "rcf-model-1", "detector-1", ModelManager.ModelType.RCF.getName(), clock, 0f),
                    new ModelState<>(thresholdingModel, "thr-model-1", "detector-1", ModelManager.ModelType.RCF.getName(), clock, 0f),
                    new ModelState<>(rcf, "rcf-model-2", "detector-2", ModelManager.ModelType.THRESHOLD.getName(), clock, 0f),
                    new ModelState<>(thresholdingModel, "thr-model-2", "detector-2", ModelManager.ModelType.THRESHOLD.getName(), clock, 0f)
                )
        );

        when(modelManager.getAllModels()).thenReturn(modelsInformation);

        ModelState<EntityModel> entityModel1 = MLUtil.randomNonEmptyModelState();
        ModelState<EntityModel> entityModel2 = MLUtil.randomNonEmptyModelState();

        List<ModelState<?>> entityModelsInformation = new ArrayList<>(Arrays.asList(entityModel1, entityModel2));
        EntityCache cache = mock(EntityCache.class);
        when(cacheProvider.get()).thenReturn(cache);
        when(cache.getAllModels()).thenReturn(entityModelsInformation);

        IndexUtils indexUtils = mock(IndexUtils.class);

        when(indexUtils.getIndexHealthStatus(anyString())).thenReturn("yellow");
        when(indexUtils.getNumberOfDocumentsInIndex(anyString())).thenReturn(100L);

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
            }
        };

        adStats = new ADStats(indexUtils, modelManager, statsMap);
    }

    @Test
    public void testStatNamesGetNames() {
        assertEquals("getNames of StatNames returns the incorrect number of stats", StatNames.getNames().size(), StatNames.values().length);
    }

    @Test
    public void testGetStats() {
        Map<String, ADStat<?>> stats = adStats.getStats();

        assertEquals("getStats returns the incorrect number of stats", stats.size(), statsMap.size());

        for (Map.Entry<String, ADStat<?>> stat : stats.entrySet()) {
            assertTrue(
                "getStats returns incorrect stats",
                adStats.getStats().containsKey(stat.getKey()) && adStats.getStats().get(stat.getKey()) == stat.getValue()
            );
        }
    }

    @Test
    public void testGetStat() {
        ADStat<?> stat = adStats.getStat(clusterStatName1);

        assertTrue(
            "getStat returns incorrect stat",
            adStats.getStats().containsKey(clusterStatName1) && adStats.getStats().get(clusterStatName1) == stat
        );
    }

    @Test
    public void testGetNodeStats() {
        Map<String, ADStat<?>> stats = adStats.getStats();
        Set<ADStat<?>> nodeStats = new HashSet<>(adStats.getNodeStats().values());

        for (ADStat<?> stat : stats.values()) {
            assertTrue(
                "getNodeStats returns incorrect stat",
                (stat.isClusterLevel() && !nodeStats.contains(stat)) || (!stat.isClusterLevel() && nodeStats.contains(stat))
            );
        }
    }

    @Test
    public void testGetClusterStats() {
        Map<String, ADStat<?>> stats = adStats.getStats();
        Set<ADStat<?>> clusterStats = new HashSet<>(adStats.getClusterStats().values());

        for (ADStat<?> stat : stats.values()) {
            assertTrue(
                "getClusterStats returns incorrect stat",
                (stat.isClusterLevel() && clusterStats.contains(stat)) || (!stat.isClusterLevel() && !clusterStats.contains(stat))
            );
        }
    }

}
