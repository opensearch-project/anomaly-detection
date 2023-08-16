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

package org.opensearch.ad.stats;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MAX_MODEL_SIZE_PER_NODE;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.caching.EntityCache;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.HybridThresholdingModel;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.stats.suppliers.CounterSupplier;
import org.opensearch.ad.stats.suppliers.IndexStatusSupplier;
import org.opensearch.ad.stats.suppliers.ModelsOnNodeSupplier;
import org.opensearch.ad.util.IndexUtils;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.stats.StatNames;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

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

        // sampleSize * numberOfTrees has to be larger than 1. Otherwise, RCF reports errors.
        rcf = RandomCutForest.builder().dimensions(1).sampleSize(2).numberOfTrees(1).build();
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

        ModelState<EntityModel> entityModel1 = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        ModelState<EntityModel> entityModel2 = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());

        List<ModelState<?>> entityModelsInformation = new ArrayList<>(Arrays.asList(entityModel1, entityModel2));
        EntityCache cache = mock(EntityCache.class);
        when(cacheProvider.get()).thenReturn(cache);
        when(cache.getAllModels()).thenReturn(entityModelsInformation);

        IndexUtils indexUtils = mock(IndexUtils.class);

        when(indexUtils.getIndexHealthStatus(anyString())).thenReturn("yellow");

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

        statsMap = new HashMap<String, ADStat<?>>() {
            {
                put(nodeStatName1, new ADStat<>(false, new CounterSupplier()));
                put(nodeStatName2, new ADStat<>(false, new ModelsOnNodeSupplier(modelManager, cacheProvider, settings, clusterService)));
                put(clusterStatName1, new ADStat<>(true, new IndexStatusSupplier(indexUtils, "index1")));
                put(clusterStatName2, new ADStat<>(true, new IndexStatusSupplier(indexUtils, "index2")));
            }
        };

        adStats = new ADStats(statsMap);
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
