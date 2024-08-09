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

package org.opensearch.timeseries.transport;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.ml.ADColdStart;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.CronAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.forecast.caching.ForecastCacheProvider;
import org.opensearch.forecast.ml.ForecastColdStart;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.transport.TransportService;

public class CronTransportAction extends TransportNodesAction<CronRequest, CronResponse, CronNodeRequest, CronNodeResponse> {
    private final Logger LOG = LogManager.getLogger(CronTransportAction.class);
    private NodeStateManager transportStateManager;
    private ADModelManager adModelManager;
    private ADCacheProvider adCacheProvider;
    private ForecastCacheProvider forecastCacheProvider;
    private ADColdStart adEntityColdStarter;
    private ForecastColdStart forecastColdStarter;
    private ADTaskManager adTaskManager;
    private ForecastTaskManager forecastTaskManager;

    @Inject
    public CronTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeStateManager tarnsportStatemanager,
        ADModelManager adModelManager,
        ADCacheProvider adCacheProvider,
        ForecastCacheProvider forecastCacheProvider,
        ADColdStart adEntityColdStarter,
        ForecastColdStart forecastColdStarter,
        ADTaskManager adTaskManager,
        ForecastTaskManager forecastTaskManager
    ) {
        super(
            CronAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            CronRequest::new,
            CronNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            CronNodeResponse.class
        );
        this.transportStateManager = tarnsportStatemanager;
        this.adModelManager = adModelManager;
        this.adCacheProvider = adCacheProvider;
        this.forecastCacheProvider = forecastCacheProvider;
        this.adEntityColdStarter = adEntityColdStarter;
        this.forecastColdStarter = forecastColdStarter;
        this.adTaskManager = adTaskManager;
        this.forecastTaskManager = forecastTaskManager;
    }

    @Override
    protected CronResponse newResponse(CronRequest request, List<CronNodeResponse> responses, List<FailedNodeException> failures) {
        return new CronResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected CronNodeRequest newNodeRequest(CronRequest request) {
        return new CronNodeRequest();
    }

    @Override
    protected CronNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new CronNodeResponse(in);
    }

    /**
     * Delete unused models and save checkpoints before deleting (including both RCF
     * and thresholding model), buffered shingle data, and transport state
     *
     * @param request delete request
     * @return delete response including local node Id.
     */
    @Override
    protected CronNodeResponse nodeOperation(CronNodeRequest request) {
        LOG.info("Start running hourly cron.");
        // ======================
        // AD
        // ======================
        // makes checkpoints for hosted models and stop hosting models not actively
        // used.
        // for single-entity detector
        adModelManager
            .maintenance(ActionListener.wrap(v -> LOG.debug("model maintenance done"), e -> LOG.error("Error maintaining ad model", e)));
        // for multi-entity detector
        adCacheProvider.get().maintenance();

        adEntityColdStarter.maintenance();
        // clean child tasks and AD results of deleted detector level task
        adTaskManager.cleanChildTasksAndResultsOfDeletedTask();

        // clean AD results of deleted detector
        adTaskManager.cleanResultOfDeletedConfig();

        // maintain running historical tasks: reset task state as stopped if not running and clean stale running entities
        adTaskManager.maintainRunningHistoricalTasks(transportService, 100);

        // maintain running realtime tasks: clean stale running realtime task cache
        adTaskManager.maintainRunningRealtimeTasks();

        // ======================
        // Forecast
        // ======================
        forecastCacheProvider.get().maintenance();
        forecastColdStarter.maintenance();
        // clean child tasks and forecast results of deleted forecaster level task
        forecastTaskManager.cleanChildTasksAndResultsOfDeletedTask();
        forecastTaskManager.cleanResultOfDeletedConfig();
        forecastTaskManager.maintainRunningRealtimeTasks();

        // ======================
        // Common
        // ======================
        // delete unused transport state
        transportStateManager.maintenance();

        return new CronNodeResponse(clusterService.localNode());
    }
}
