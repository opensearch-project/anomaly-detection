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

package org.opensearch.ad.stats.suppliers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MAX_MODEL_SIZE_PER_NODE;
import static org.opensearch.ad.stats.suppliers.ADModelsOnNodeSupplier.MODEL_STATE_STAT_KEYS;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.ml.HybridThresholdingModel;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

public class ModelsOnNodeSupplierTests extends OpenSearchTestCase {
    private RandomCutForest rcf;
    private HybridThresholdingModel thresholdingModel;
    private List<ModelState<?>> expectedResults;
    private Clock clock;
    private List<ModelState<ThresholdedRandomCutForest>> entityModelsInformation;

    @Mock
    private ADModelManager modelManager;

    @Mock
    private ADCacheProvider cacheProvider;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        clock = Clock.systemUTC();
        rcf = RandomCutForest.builder().dimensions(1).sampleSize(2).numberOfTrees(1).build();
        thresholdingModel = new HybridThresholdingModel(1e-8, 1e-5, 200, 10_000, 2, 5_000_000);

        expectedResults = new ArrayList<>(
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

        when(modelManager.getAllModels()).thenReturn(expectedResults);

        ModelState<ThresholdedRandomCutForest> entityModel1 = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        ModelState<ThresholdedRandomCutForest> entityModel2 = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());

        entityModelsInformation = new ArrayList<>(Arrays.asList(entityModel1, entityModel2));
        ADPriorityCache cache = mock(ADPriorityCache.class);
        when(cacheProvider.get()).thenReturn(cache);
        when(cache.getAllModels()).thenReturn(entityModelsInformation);
    }

    @Test
    public void testGet() {
        Settings settings = Settings.builder().put(AD_MAX_MODEL_SIZE_PER_NODE.getKey(), 10).build();
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AD_MAX_MODEL_SIZE_PER_NODE)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        ADModelsOnNodeSupplier modelsOnNodeSupplier = new ADModelsOnNodeSupplier(modelManager, cacheProvider, settings, clusterService);
        List<Map<String, Object>> results = modelsOnNodeSupplier.get();
        assertEquals(
            "get fails to return correct result",
            Stream
                .concat(expectedResults.stream(), entityModelsInformation.stream())
                .map(
                    modelState -> modelState
                        .getModelStateAsMap()
                        .entrySet()
                        .stream()
                        .filter(entry -> MODEL_STATE_STAT_KEYS.contains(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                )
                .collect(Collectors.toList()),
            results
        );
    }

    @Test
    public void testGetModelCount() {
        ADModelsOnNodeCountSupplier modelsOnNodeSupplier = new ADModelsOnNodeCountSupplier(modelManager, cacheProvider);
        assertEquals(6L, modelsOnNodeSupplier.get().longValue());
    }
}
