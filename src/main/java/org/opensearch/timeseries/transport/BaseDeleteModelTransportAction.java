/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.caching.CacheProvider;
import org.opensearch.timeseries.caching.TimeSeriesCache;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.ModelColdStart;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class BaseDeleteModelTransportAction<RCFModelType extends ThresholdedRandomCutForest, CacheType extends TimeSeriesCache<RCFModelType>, CacheProviderType extends CacheProvider<RCFModelType, CacheType>, TaskCacheManagerType extends TaskCacheManager, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriteWorkerType extends CheckpointWriteWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, IndexableResultType extends IndexableResult, ModelColdStartType extends ModelColdStart<RCFModelType, IndexType, IndexManagementType, IndexableResultType>>
    extends TransportNodesAction<DeleteModelRequest, DeleteModelResponse, DeleteModelNodeRequest, DeleteModelNodeResponse> {

    private static final Logger LOG = LogManager.getLogger(BaseDeleteModelTransportAction.class);
    private NodeStateManager nodeStateManager;
    private CacheProviderType cache;
    private TaskCacheManagerType adTaskCacheManager;
    private ModelColdStartType coldStarter;

    public BaseDeleteModelTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeStateManager nodeStateManager,
        CacheProviderType cache,
        TaskCacheManagerType taskCacheManager,
        ModelColdStartType coldStarter,
        String deleteModelAction
    ) {
        super(
            deleteModelAction,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            DeleteModelRequest::new,
            DeleteModelNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            DeleteModelNodeResponse.class
        );
        this.nodeStateManager = nodeStateManager;
        this.cache = cache;
        this.adTaskCacheManager = taskCacheManager;
        this.coldStarter = coldStarter;
    }

    @Override
    protected DeleteModelResponse newResponse(
        DeleteModelRequest request,
        List<DeleteModelNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new DeleteModelResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected DeleteModelNodeRequest newNodeRequest(DeleteModelRequest request) {
        return new DeleteModelNodeRequest(request);
    }

    @Override
    protected DeleteModelNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new DeleteModelNodeResponse(in);
    }

    /**
     *
     * Delete checkpoint document (including both RCF and thresholding model), in-memory models,
     * buffered shingle data, transport state, and anomaly result
     *
     * @param request delete request
     * @return delete response including local node Id.
     */
    @Override
    protected DeleteModelNodeResponse nodeOperation(DeleteModelNodeRequest request) {

        String configID = request.getConfigID();
        LOG.info("Delete model for {}", configID);
        nodeStateManager.clear(configID);

        cache.get().clear(configID);

        coldStarter.clear(configID);

        // delete realtime task cache
        adTaskCacheManager.removeRealtimeTaskCache(configID);

        LOG.info("Finished deleting {}", configID);
        return new DeleteModelNodeResponse(clusterService.localNode());
    }
}
