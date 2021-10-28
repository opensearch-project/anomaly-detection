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

import static org.opensearch.ad.ml.ModelState.LAST_CHECKPOINT_TIME_KEY;
import static org.opensearch.ad.ml.ModelState.LAST_USED_TIME_KEY;
import static org.opensearch.ad.ml.ModelState.MODEL_TYPE_KEY;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_MODEL_SIZE_PER_NODE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;

/**
 * ModelsOnNodeSupplier provides a List of ModelStates info for the models the nodes contains
 */
public class ModelsOnNodeSupplier implements Supplier<List<Map<String, Object>>> {
    private ModelManager modelManager;
    private CacheProvider cache;
    // the max number of models to return per node. Defaults to 100.
    private volatile int numModelsToReturn;

    /**
     * Set that contains the model stats that should be exposed.
     */
    public static Set<String> MODEL_STATE_STAT_KEYS = new HashSet<>(
        Arrays
            .asList(
                CommonName.MODEL_ID_KEY,
                CommonName.DETECTOR_ID_KEY,
                MODEL_TYPE_KEY,
                CommonName.ENTITY_KEY,
                LAST_USED_TIME_KEY,
                LAST_CHECKPOINT_TIME_KEY
            )
    );

    /**
     * Constructor
     *
     * @param modelManager object that manages the model partitions hosted on the node
     * @param cache object that manages multi-entity detectors' models
     * @param settings node settings accessor
     * @param clusterService Cluster service accessor
     */
    public ModelsOnNodeSupplier(ModelManager modelManager, CacheProvider cache, Settings settings, ClusterService clusterService) {
        this.modelManager = modelManager;
        this.cache = cache;
        this.numModelsToReturn = MAX_MODEL_SIZE_PER_NODE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_MODEL_SIZE_PER_NODE, it -> this.numModelsToReturn = it);
    }

    @Override
    public List<Map<String, Object>> get() {
        List<Map<String, Object>> values = new ArrayList<>();
        Stream
            .concat(modelManager.getAllModels().stream(), cache.get().getAllModels().stream())
            .limit(numModelsToReturn)
            .forEach(
                modelState -> values
                    .add(
                        modelState
                            .getModelStateAsMap()
                            .entrySet()
                            .stream()
                            .filter(entry -> MODEL_STATE_STAT_KEYS.contains(entry.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                    )
            );

        return values;
    }
}
