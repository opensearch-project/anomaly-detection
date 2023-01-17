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

package org.opensearch.ad.transport;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MAX_MODEL_SIZE_PER_NODE;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.transport.BaseProfileTransportAction;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 *  This class contains the logic to extract the stats from the nodes
 */
public class ADProfileTransportAction extends BaseProfileTransportAction<ThresholdedRandomCutForest, ADPriorityCache, ADCacheProvider> {

    /**
     * Constructor
     *
     * @param threadPool ThreadPool to use
     * @param clusterService ClusterService
     * @param transportService TransportService
     * @param actionFilters Action Filters
     * @param taskCacheManager task cache manager object
     * @param cacheProvider cache provider
     * @param settings Node settings accessor
     */
    @Inject
    public ADProfileTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ADTaskCacheManager taskCacheManager,
        ADCacheProvider cacheProvider,
        Settings settings
    ) {
        super(
            ADProfileAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            cacheProvider,
            settings,
            AD_MAX_MODEL_SIZE_PER_NODE,
            taskCacheManager
        );
    }
}
