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

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.ml.EntityColdStarter;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class CronTransportAction extends TransportNodesAction<CronRequest, CronResponse, CronNodeRequest, CronNodeResponse> {
    private final Logger LOG = LogManager.getLogger(CronTransportAction.class);
    private NodeStateManager transportStateManager;
    private ModelManager modelManager;
    private FeatureManager featureManager;
    private CacheProvider cacheProvider;
    private EntityColdStarter entityColdStarter;
    private ADTaskManager adTaskManager;

    @Inject
    public CronTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeStateManager tarnsportStatemanager,
        ModelManager modelManager,
        FeatureManager featureManager,
        CacheProvider cacheProvider,
        EntityColdStarter entityColdStarter,
        ADTaskManager adTaskManager
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
        this.modelManager = modelManager;
        this.featureManager = featureManager;
        this.cacheProvider = cacheProvider;
        this.entityColdStarter = entityColdStarter;
        this.adTaskManager = adTaskManager;
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
        LOG.info("Start running AD hourly cron.");
        // makes checkpoints for hosted models and stop hosting models not actively
        // used.
        // for single-entity detector
        modelManager
            .maintenance(ActionListener.wrap(v -> LOG.debug("model maintenance done"), e -> LOG.error("Error maintaining model", e)));
        // for multi-entity detector
        cacheProvider.get().maintenance();

        // delete unused buffered shingle data
        featureManager.maintenance();

        // delete unused transport state
        transportStateManager.maintenance();

        entityColdStarter.maintenance();
        // clean child tasks and AD results of deleted detector level task
        adTaskManager.cleanChildTasksAndADResultsOfDeletedTask();

        // clean AD results of deleted detector
        adTaskManager.cleanADResultOfDeletedDetector();

        // maintain running historical tasks: reset task state as stopped if not running and clean stale running entities
        adTaskManager.maintainRunningHistoricalTasks(transportService, 100);

        // maintain running realtime tasks: clean stale running realtime task cache
        adTaskManager.maintainRunningRealtimeTasks();

        return new CronNodeResponse(clusterService.localNode());
    }
}
