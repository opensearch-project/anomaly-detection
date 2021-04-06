/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.NodeStateManager;
import com.amazon.opendistroforelasticsearch.ad.caching.CacheProvider;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;

public class DeleteModelTransportAction extends
    TransportNodesAction<DeleteModelRequest, DeleteModelResponse, DeleteModelNodeRequest, DeleteModelNodeResponse> {
    private static final Logger LOG = LogManager.getLogger(DeleteModelTransportAction.class);
    private NodeStateManager transportStateManager;
    private ModelManager modelManager;
    private FeatureManager featureManager;
    private CacheProvider cache;

    @Inject
    public DeleteModelTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        NodeStateManager tarnsportStatemanager,
        ModelManager modelManager,
        FeatureManager featureManager,
        CacheProvider cache
    ) {
        super(
            DeleteModelAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            DeleteModelRequest::new,
            DeleteModelNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            DeleteModelNodeResponse.class
        );
        this.transportStateManager = tarnsportStatemanager;
        this.modelManager = modelManager;
        this.featureManager = featureManager;
        this.cache = cache;
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

        String adID = request.getAdID();
        LOG.info("Delete model for {}", adID);
        // delete in-memory models and model checkpoint
        modelManager.clear(adID);

        // delete buffered shingle data
        featureManager.clear(adID);

        // delete transport state
        transportStateManager.clear(adID);

        cache.get().clear(adID);

        LOG.info("Finished deleting {}", adID);
        return new DeleteModelNodeResponse(clusterService.localNode());
    }

}
