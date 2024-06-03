/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.stats;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_MAX_MODEL_SIZE_PER_NODE;
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

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.caching.ForecastCacheProvider;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.timeseries.constant.CommonName;

public class ForecastModelsOnNodeSupplier implements Supplier<List<Map<String, Object>>> {
    private ForecastCacheProvider forecastCache;
    private volatile int forecastNumModelsToReturn;

    /**
     * Set that contains the model stats that should be exposed.
     */
    public static Set<String> MODEL_STATE_STAT_KEYS = new HashSet<>(
        Arrays
            .asList(
                CommonName.MODEL_ID_FIELD,
                MODEL_TYPE_KEY,
                CommonName.ENTITY_KEY,
                LAST_USED_TIME_KEY,
                LAST_CHECKPOINT_TIME_KEY,
                ForecastCommonName.FORECASTER_ID_KEY
            )
    );

    /**
     * Constructor
     *
     * @param forecastCache object that manages HC forecasters' models
     * @param settings node settings accessor
     * @param clusterService Cluster service accessor
     */
    public ForecastModelsOnNodeSupplier(ForecastCacheProvider forecastCache, Settings settings, ClusterService clusterService) {
        this.forecastCache = forecastCache;
        this.forecastNumModelsToReturn = FORECAST_MAX_MODEL_SIZE_PER_NODE.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(FORECAST_MAX_MODEL_SIZE_PER_NODE, it -> this.forecastNumModelsToReturn = it);
    }

    @Override
    public List<Map<String, Object>> get() {
        Stream<Map<String, Object>> forecastStream = forecastCache
            .get()
            .getAllModels()
            .stream()
            .limit(forecastNumModelsToReturn)
            .map(
                modelState -> modelState
                    .getModelStateAsMap()
                    .entrySet()
                    .stream()
                    .filter(entry -> MODEL_STATE_STAT_KEYS.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );

        return forecastStream.collect(Collectors.toList());
    }
}
