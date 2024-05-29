/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.caching;

import org.opensearch.common.inject.Provider;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * A wrapper to call concrete implementation of caching.  Used in transport
 * action.  Don't use interface because transport action handler constructor
 * requires a concrete class as input.
 *
 */
public class CacheProvider<RCFModelType extends ThresholdedRandomCutForest, CacheType extends TimeSeriesCache<RCFModelType>>
    implements
        Provider<CacheType> {
    private CacheType cache;

    public CacheProvider() {

    }

    @Override
    public CacheType get() {
        return cache;
    }

    public void set(CacheType cache) {
        this.cache = cache;
    }
}
