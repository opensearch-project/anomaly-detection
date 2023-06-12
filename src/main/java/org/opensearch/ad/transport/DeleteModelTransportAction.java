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

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.ml.EntityColdStarter;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.threadpool.ThreadPool;

import com.google.inject.Inject;

public class DeleteModelTransportAction extends TransportAction<DeleteModelRequest, DeleteModelResponse> {
    private static final Logger LOG = LogManager.getLogger(DeleteModelTransportAction.class);
    private NodeStateManager nodeStateManager;
    private ModelManager modelManager;
    private FeatureManager featureManager;
    private CacheProvider cache;
    private ADTaskCacheManager adTaskCacheManager;
    private EntityColdStarter coldStarter;
    private SDKClusterService clusterService;
    private ExtensionsRunner extensionsRunner;

    @Inject
    public DeleteModelTransportAction(
        ThreadPool threadPool,
        SDKClusterService clusterService,
        TaskManager taskManager,
        ActionFilters actionFilters,
        NodeStateManager nodeStateManager,
        ModelManager modelManager,
        FeatureManager featureManager,
        CacheProvider cache,
        ADTaskCacheManager adTaskCacheManager,
        EntityColdStarter coldStarter,
        ExtensionsRunner extensionsRunner
    ) {
        super(DeleteModelAction.NAME, actionFilters, taskManager);
        this.clusterService = clusterService;
        this.nodeStateManager = nodeStateManager;
        this.modelManager = modelManager;
        this.featureManager = featureManager;
        this.cache = cache;
        this.adTaskCacheManager = adTaskCacheManager;
        this.coldStarter = coldStarter;
        this.extensionsRunner = extensionsRunner;
    }

    @Override
    protected void doExecute(Task task, DeleteModelRequest request, ActionListener<DeleteModelResponse> actionListener) {
        // Delete checkpoint document (including both RCF and thresholding model), in-memory models,
        // buffered shingle data, transport state, and anomaly result
        String adID = request.getAdID();
        LOG.info("Delete model for {}", adID);
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

        // delete buffered shingle data
        featureManager.clear(adID);

        // delete transport state
        nodeStateManager.clear(adID);

        cache.get().clear(adID);

        coldStarter.clear(adID);

        // delete realtime task cache
        adTaskCacheManager.removeRealtimeTaskCache(adID);

        LOG.info("Finished deleting {}", adID);
        DeleteModelNodeResponse deleteModelNodeResponse = new DeleteModelNodeResponse(clusterService.localNode());
        actionListener.onResponse(newResponse(request, List.of(deleteModelNodeResponse), new ArrayList<>()));
    }

    protected DeleteModelResponse newResponse(
        DeleteModelRequest request,
        List<DeleteModelNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new DeleteModelResponse(clusterService.getClusterName(), responses, failures);
    }
}
