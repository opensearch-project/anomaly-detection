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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ml.ADColdStart;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.transport.BaseDeleteModelTransportAction;
import org.opensearch.timeseries.transport.DeleteModelNodeRequest;
import org.opensearch.timeseries.transport.DeleteModelNodeResponse;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class DeleteADModelTransportAction extends
    BaseDeleteModelTransportAction<ThresholdedRandomCutForest, ADPriorityCache, ADCacheProvider, ADTaskCacheManager, ADIndex, ADIndexManagement, ADCheckpointDao, ADCheckpointWriteWorker, ADColdStart> {
    private static final Logger LOG = LogManager.getLogger(DeleteADModelTransportAction.class);
    private ADModelManager modelManager;
    private FeatureManager featureManager;

    @Inject
    public DeleteADModelTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeStateManager nodeStateManager,
        ADModelManager modelManager,
        FeatureManager featureManager,
        ADCacheProvider cache,
        ADTaskCacheManager adTaskCacheManager,
        ADColdStart coldStarter
    ) {
        super(
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            nodeStateManager,
            cache,
            adTaskCacheManager,
            coldStarter,
            DeleteADModelAction.NAME
        );
        this.modelManager = modelManager;
        this.featureManager = featureManager;
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
        super.nodeOperation(request);
        String adID = request.getConfigID();

        // delete in-memory models and model checkpoint
        modelManager
            .clear(
                adID,
                ActionListener
                    .wrap(
                        r -> LOG.info("Deleted model for [{}] with response [{}] ", adID, r),
                        e -> LOG.error("Fail to delete model for " + adID, e)
                    )
            );

        LOG.info("Finished deleting ad models for {}", adID);
        return new DeleteModelNodeResponse(clusterService.localNode());
    }

}
