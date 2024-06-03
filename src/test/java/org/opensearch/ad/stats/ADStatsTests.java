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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.ml.HybridThresholdingModel;
import org.opensearch.ad.stats.suppliers.ADModelsOnNodeSupplier;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.TimeSeriesStat;
import org.opensearch.timeseries.stats.suppliers.CounterSupplier;
import org.opensearch.timeseries.stats.suppliers.IndexStatusSupplier;
import org.opensearch.timeseries.util.IndexUtils;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

public class ADStatsTests extends OpenSearchTestCase {

    private Map<String, TimeSeriesStat<?>> statsMap;
    private ADStats adStats;
    private RandomCutForest rcf;
    private HybridThresholdingModel thresholdingModel;
    private String clusterStatName1, clusterStatName2;
    private String nodeStatName1, nodeStatName2;

    @Mock
    private Clock clock;

    @Mock
    private ADModelManager modelManager;

    @Mock
    private ADCacheProvider cacheProvider;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        // sampleSize * numberOfTrees has to be larger than 1. Otherwise, RCF reports errors.
        rcf = RandomCutForest.builder().dimensions(1).sampleSize(2).numberOfTrees(1).build();
        thresholdingModel = new HybridThresholdingModel(1e-8, 1e-5, 200, 10_000, 2, 5_000_000);

        List<ModelState<?>> modelsInformation = new ArrayList<>(
            Arrays
                .asList(
                    new ModelState<>(
                        rcf,
                        "rcf-model-1",
                        "detector-1",
                        ModelManager.ModelType.RCF.getName(),
                        clock,
                        0f,
                        Optional.empty(),
                        new ArrayDeque<>()
                    ),
                    new ModelState<>(
                        thresholdingModel,
                        "thr-model-1",
                        "detector-1",
                        ModelManager.ModelType.RCF.getName(),
                        clock,
                        0f,
                        Optional.empty(),
                        new ArrayDeque<>()
                    ),
                    new ModelState<>(
                        rcf,
                        "rcf-model-2",
                        "detector-2",
                        ModelManager.ModelType.THRESHOLD.getName(),
                        clock,
                        0f,
                        Optional.empty(),
                        new ArrayDeque<>()
                    ),
                    new ModelState<>(
                        thresholdingModel,
                        "thr-model-2",
                        "detector-2",
                        ModelManager.ModelType.THRESHOLD.getName(),
                        clock,
                        0f,
                        Optional.empty(),
                        new ArrayDeque<>()
                    )
                )
        );

        when(modelManager.getAllModels()).thenReturn(modelsInformation);

        ModelState<ThresholdedRandomCutForest> entityModel1 = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        ModelState<ThresholdedRandomCutForest> entityModel2 = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());

        List<ModelState<ThresholdedRandomCutForest>> entityModelsInformation = new ArrayList<>(Arrays.asList(entityModel1, entityModel2));
        ADPriorityCache cache = mock(ADPriorityCache.class);
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

        statsMap = new HashMap<String, TimeSeriesStat<?>>() {
            {
                put(nodeStatName1, new TimeSeriesStat<>(false, new CounterSupplier()));
                put(
                    nodeStatName2,
                    new TimeSeriesStat<>(false, new ADModelsOnNodeSupplier(modelManager, cacheProvider, settings, clusterService))
                );
                put(clusterStatName1, new TimeSeriesStat<>(true, new IndexStatusSupplier(indexUtils, "index1")));
                put(clusterStatName2, new TimeSeriesStat<>(true, new IndexStatusSupplier(indexUtils, "index2")));
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
        Map<String, TimeSeriesStat<?>> stats = adStats.getStats();

        assertEquals("getStats returns the incorrect number of stats", stats.size(), statsMap.size());

        for (Map.Entry<String, TimeSeriesStat<?>> stat : stats.entrySet()) {
            assertTrue(
                "getStats returns incorrect stats",
                adStats.getStats().containsKey(stat.getKey()) && adStats.getStats().get(stat.getKey()) == stat.getValue()
            );
        }
    }

    @Test
    public void testGetStat() {
        TimeSeriesStat<?> stat = adStats.getStat(clusterStatName1);

        assertTrue(
            "getStat returns incorrect stat",
            adStats.getStats().containsKey(clusterStatName1) && adStats.getStats().get(clusterStatName1) == stat
        );
    }

    @Test
    public void testGetNodeStats() {
        Map<String, TimeSeriesStat<?>> stats = adStats.getStats();
        Set<TimeSeriesStat<?>> nodeStats = new HashSet<>(adStats.getNodeStats().values());

        for (TimeSeriesStat<?> stat : stats.values()) {
            assertTrue(
                "getNodeStats returns incorrect stat",
                (stat.isClusterLevel() && !nodeStats.contains(stat)) || (!stat.isClusterLevel() && nodeStats.contains(stat))
            );
        }
    }

    @Test
    public void testGetClusterStats() {
        Map<String, TimeSeriesStat<?>> stats = adStats.getStats();
        Set<TimeSeriesStat<?>> clusterStats = new HashSet<>(adStats.getClusterStats().values());

        for (TimeSeriesStat<?> stat : stats.values()) {
            assertTrue(
                "getClusterStats returns incorrect stat",
                (stat.isClusterLevel() && clusterStats.contains(stat)) || (!stat.isClusterLevel() && !clusterStats.contains(stat))
            );
        }
    }

}
