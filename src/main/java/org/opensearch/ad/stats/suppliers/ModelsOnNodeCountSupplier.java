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

import java.util.function.Supplier;
import java.util.stream.Stream;

import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.ml.ModelManager;

/**
 * ModelsOnNodeCountSupplier provides the number of models a node contains
 */
public class ModelsOnNodeCountSupplier implements Supplier<Long> {
    private ModelManager modelManager;
    private CacheProvider cache;

    /**
     * Constructor
     *
     * @param modelManager object that manages the model partitions hosted on the node
     * @param cache object that manages multi-entity detectors' models
     */
    public ModelsOnNodeCountSupplier(ModelManager modelManager, CacheProvider cache) {
        this.modelManager = modelManager;
        this.cache = cache;
    }

    @Override
    public Long get() {
        return Stream.concat(modelManager.getAllModels().stream(), cache.get().getAllModels().stream()).count();
    }
}
