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
import org.opensearch.forecast.caching.ForecastCacheProvider;
import org.opensearch.forecast.caching.ForecastPriorityCache;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ml.ForecastColdStart;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.ratelimit.ForecastCheckpointWriteWorker;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.BaseDeleteModelTransportAction;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class DeleteForecastModelTransportAction extends
    BaseDeleteModelTransportAction<RCFCaster, ForecastPriorityCache, ForecastCacheProvider, TaskCacheManager, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastCheckpointWriteWorker, ForecastResult, ForecastColdStart> {

    @Inject
    public DeleteForecastModelTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeStateManager nodeStateManager,
        ForecastCacheProvider cache,
        TaskCacheManager taskCacheManager,
        ForecastColdStart coldStarter
    ) {
        super(
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            nodeStateManager,
            cache,
            taskCacheManager,
            coldStarter,
            DeleteForecastModelAction.NAME
        );
    }
}
