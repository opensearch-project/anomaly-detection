/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.caching;

import org.opensearch.timeseries.caching.CacheProvider;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Allows Guice dependency based on types. Otherwise, Guice cannot
 * decide which instance to inject based on generic types of CacheProvider
 *
 */
public class ADCacheProvider extends CacheProvider<ThresholdedRandomCutForest, ADPriorityCache> {

}
