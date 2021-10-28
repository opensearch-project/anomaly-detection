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

package org.opensearch.ad.caching;

import org.opensearch.common.inject.Provider;

/**
 * A wrapper to call concrete implementation of caching.  Used in transport
 * action.  Don't use interface because transport action handler constructor
 * requires a concrete class as input.
 *
 */
public class CacheProvider implements Provider<EntityCache> {
    private EntityCache cache;

    public CacheProvider(EntityCache cache) {
        this.cache = cache;
    }

    @Override
    public EntityCache get() {
        return cache;
    }
}
