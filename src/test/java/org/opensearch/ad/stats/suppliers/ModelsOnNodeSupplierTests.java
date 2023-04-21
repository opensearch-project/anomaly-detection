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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_MODEL_SIZE_PER_NODE;
import static org.opensearch.ad.stats.suppliers.ModelsOnNodeSupplier.MODEL_STATE_STAT_KEYS;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKClusterService.SDKClusterSettings;
import org.opensearch.test.OpenSearchTestCase;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

import com.amazon.randomcutforest.RandomCutForest;

public class ModelsOnNodeSupplierTests extends OpenSearchTestCase {
    private RandomCutForest rcf;
    private HybridThresholdingModel thresholdingModel;
    private List<ModelState<?>> expectedResults;
    private Clock clock;
    private List<ModelState<?>> entityModelsInformation;

    @Mock
    private ModelManager modelManager;

    @Mock
    private CacheProvider cacheProvider;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        clock = Clock.systemUTC();
        rcf = RandomCutForest.builder().dimensions(1).sampleSize(2).numberOfTrees(1).build();
        thresholdingModel = new HybridThresholdingModel(1e-8, 1e-5, 200, 10_000, 2, 5_000_000);

        expectedResults = new ArrayList<>(
            Arrays
                .asList(
                    new ModelState<>(rcf, "rcf-model-1", "detector-1", ModelManager.ModelType.RCF.getName(), clock, 0f),
                    new ModelState<>(thresholdingModel, "thr-model-1", "detector-1", ModelManager.ModelType.RCF.getName(), clock, 0f),
                    new ModelState<>(rcf, "rcf-model-2", "detector-2", ModelManager.ModelType.THRESHOLD.getName(), clock, 0f),
                    new ModelState<>(thresholdingModel, "thr-model-2", "detector-2", ModelManager.ModelType.THRESHOLD.getName(), clock, 0f)
                )
        );

        when(modelManager.getAllModels()).thenReturn(expectedResults);

        ModelState<EntityModel> entityModel1 = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        ModelState<EntityModel> entityModel2 = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());

        entityModelsInformation = new ArrayList<>(Arrays.asList(entityModel1, entityModel2));
        EntityCache cache = mock(EntityCache.class);
        when(cacheProvider.get()).thenReturn(cache);
        when(cache.getAllModels()).thenReturn(entityModelsInformation);
    }

    @Test
    public void testGet() {
        Settings settings = Settings.builder().put(MAX_MODEL_SIZE_PER_NODE.getKey(), 10).build();
        SDKClusterService clusterService = mock(SDKClusterService.class);
        SDKClusterSettings clusterSettings = spy(
            clusterService.new SDKClusterSettings(
                Settings.EMPTY, Collections.unmodifiableSet(new HashSet<>(Arrays.asList(MAX_MODEL_SIZE_PER_NODE)))
            )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        doNothing().when(clusterSettings).addSettingsUpdateConsumer(any(Setting.class), any(Consumer.class));

        ModelsOnNodeSupplier modelsOnNodeSupplier = new ModelsOnNodeSupplier(modelManager, cacheProvider, settings, clusterService);
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
        ModelsOnNodeCountSupplier modelsOnNodeSupplier = new ModelsOnNodeCountSupplier(modelManager, cacheProvider);
        assertEquals(6L, modelsOnNodeSupplier.get().longValue());
    }
}
