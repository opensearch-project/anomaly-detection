/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.stats.suppliers;

import java.util.function.Supplier;

import org.opensearch.forecast.caching.ForecastCacheProvider;

/**
 * ModelsOnNodeCountSupplier provides the number of models a node contains
 */
public class ForecastModelsOnNodeCountSupplier implements Supplier<Long> {
    private ForecastCacheProvider forecastCache;

    /**
     * Constructor
     *
     * @param forecastCache object that manages models
     */
    public ForecastModelsOnNodeCountSupplier(ForecastCacheProvider forecastCache) {
        this.forecastCache = forecastCache;
    }

    @Override
    public Long get() {
        return forecastCache.get().getAllModels().stream().count();
    }
}
