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

package org.opensearch.forecast.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.caching.ForecastCacheProvider;
import org.opensearch.forecast.caching.ForecastPriorityCache;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.transport.BaseEntityProfileTransportAction;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.RCFCaster;

/**
 * Transport action to get entity profile.
 */
public class ForecastEntityProfileTransportAction extends
    BaseEntityProfileTransportAction<RCFCaster, ForecastPriorityCache, ForecastCacheProvider> {

    @Inject
    public ForecastEntityProfileTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        Settings settings,
        HashRing hashRing,
        ClusterService clusterService,
        ForecastCacheProvider cacheProvider
    ) {
        super(
            actionFilters,
            transportService,
            settings,
            hashRing,
            clusterService,
            cacheProvider,
            ForecastEntityProfileAction.NAME,
            ForecastSettings.FORECAST_REQUEST_TIMEOUT
        );
    }
}
