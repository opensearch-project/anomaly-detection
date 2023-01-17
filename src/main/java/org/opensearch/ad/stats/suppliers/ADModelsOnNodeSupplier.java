/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.stats.suppliers;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MAX_MODEL_SIZE_PER_NODE;
import static org.opensearch.timeseries.ml.ModelState.LAST_CHECKPOINT_TIME_KEY;
import static org.opensearch.timeseries.ml.ModelState.LAST_USED_TIME_KEY;
import static org.opensearch.timeseries.ml.ModelState.MODEL_TYPE_KEY;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.constant.CommonName;

public class ADModelsOnNodeSupplier implements Supplier<List<Map<String, Object>>> {
    private ADModelManager modelManager;
    private ADCacheProvider adCache;
    // the max number of models to return per node. Defaults to 100.
    private volatile int adNumModelsToReturn;

    /**
     * Set that contains the model stats that should be exposed.
     */
    public static Set<String> MODEL_STATE_STAT_KEYS = new HashSet<>(
        Arrays
            .asList(
                CommonName.MODEL_ID_FIELD,
                ADCommonName.DETECTOR_ID_KEY,
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
     * @param adCache object that manages multi-entity detectors' models
     * @param settings node settings accessor
     * @param clusterService Cluster service accessor
     */
    public ADModelsOnNodeSupplier(ADModelManager modelManager, ADCacheProvider adCache, Settings settings, ClusterService clusterService) {
        this.modelManager = modelManager;
        this.adCache = adCache;
        this.adNumModelsToReturn = AD_MAX_MODEL_SIZE_PER_NODE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_MAX_MODEL_SIZE_PER_NODE, it -> this.adNumModelsToReturn = it);

    }

    @Override
    public List<Map<String, Object>> get() {
        Stream<Map<String, Object>> adStream = Stream
            .concat(modelManager.getAllModels().stream(), adCache.get().getAllModels().stream())
            .limit(adNumModelsToReturn)
            .map(
                modelState -> modelState
                    .getModelStateAsMap()
                    .entrySet()
                    .stream()
                    .filter(entry -> MODEL_STATE_STAT_KEYS.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );

        return adStream.collect(Collectors.toList());
    }
}
