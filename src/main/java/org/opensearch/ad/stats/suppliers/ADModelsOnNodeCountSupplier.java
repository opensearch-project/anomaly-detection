/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.stats.suppliers;

import java.util.function.Supplier;
import java.util.stream.Stream;

import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.ml.ADModelManager;

/**
 * ModelsOnNodeCountSupplier provides the number of models a node contains
 */
public class ADModelsOnNodeCountSupplier implements Supplier<Long> {
    private ADModelManager modelManager;
    private ADCacheProvider adCache;

    /**
     * Constructor
     *
     * @param modelManager object that manages the model partitions hosted on the node
     * @param adCache object that manages multi-entity detectors' models
     */
    public ADModelsOnNodeCountSupplier(ADModelManager modelManager, ADCacheProvider adCache) {
        this.modelManager = modelManager;
        this.adCache = adCache;
    }

    @Override
    public Long get() {
        return Stream.concat(modelManager.getAllModels().stream(), adCache.get().getAllModels().stream()).count();
    }
}
