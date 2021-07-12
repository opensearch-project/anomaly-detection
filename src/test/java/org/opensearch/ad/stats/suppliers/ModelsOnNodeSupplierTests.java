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

package org.opensearch.ad.stats.suppliers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.stats.suppliers.ModelsOnNodeSupplier.MODEL_STATE_STAT_KEYS;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
        rcf = RandomCutForest.builder().dimensions(1).sampleSize(1).numberOfTrees(1).build();
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
        ModelsOnNodeSupplier modelsOnNodeSupplier = new ModelsOnNodeSupplier(modelManager, cacheProvider);
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
}
