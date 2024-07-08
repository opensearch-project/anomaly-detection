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

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_MAX_MODEL_SIZE_PER_NODE;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.caching.ForecastCacheProvider;
import org.opensearch.forecast.caching.ForecastPriorityCache;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.BaseProfileTransportAction;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.RCFCaster;

/**
 *  This class contains the logic to extract the stats from the nodes
 */
public class ForecastProfileTransportAction extends BaseProfileTransportAction<RCFCaster, ForecastPriorityCache, ForecastCacheProvider> {

    /**
     * Constructor
     *
     * @param threadPool ThreadPool to use
     * @param clusterService ClusterService
     * @param transportService TransportService
     * @param actionFilters Action Filters
     * @param cacheProvider cache provider
     * @param settings Node settings accessor
     * @param taskCacheManager task cache manager
     */
    @Inject
    public ForecastProfileTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ForecastCacheProvider cacheProvider,
        Settings settings,
        TaskCacheManager taskCacheManager
    ) {
        super(
            ForecastProfileAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            cacheProvider,
            settings,
            FORECAST_MAX_MODEL_SIZE_PER_NODE,
            taskCacheManager
        );
    }
}
