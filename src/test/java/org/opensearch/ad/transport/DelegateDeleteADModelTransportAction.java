/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.ml.ADColdStart;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.transport.DeleteModelNodeResponse;
import org.opensearch.timeseries.transport.DeleteModelRequest;
import org.opensearch.timeseries.transport.DeleteModelResponse;
import org.opensearch.transport.TransportService;

/**
 * This utility class serves as a delegate for testing ProfileTransportAction functionalities.
 * It facilitates the invocation of protected methods within the org.opensearch.ad.transport.DeleteADModelTransportAction
 * and org.opensearch.timeseries.transport.BaseDeleteModelTransportAction classes, which are otherwise inaccessible
 * due to Java's access control restrictions. This is achieved by extending the target classes or using reflection
 * where inheritance is not possible, enabling the testing framework to perform comprehensive tests on protected
 * class members across different packages.
 */
public class DelegateDeleteADModelTransportAction extends DeleteADModelTransportAction {
    public DelegateDeleteADModelTransportAction(
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
            modelManager,
            featureManager,
            cache,
            adTaskCacheManager,
            coldStarter
        );
    }

    @Override
    public DeleteModelResponse newResponse(
        DeleteModelRequest request,
        List<DeleteModelNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return super.newResponse(request, responses, failures);
    }
}
