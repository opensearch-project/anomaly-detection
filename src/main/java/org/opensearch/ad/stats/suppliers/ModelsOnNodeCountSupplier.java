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

import java.util.function.Supplier;
import java.util.stream.Stream;

import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.ml.ModelManager;

/**
 * ModelsOnNodeSupplier provides a List of ModelStates info for the models the nodes contains
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
