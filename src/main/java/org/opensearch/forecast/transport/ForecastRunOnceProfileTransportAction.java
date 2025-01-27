/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.forecast.ratelimit.ForecastCheckpointReadWorker;
import org.opensearch.forecast.ratelimit.ForecastColdStartWorker;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.transport.TransportService;

public class ForecastRunOnceProfileTransportAction extends
    TransportNodesAction<ForecastRunOnceProfileRequest, ForecastRunOnceProfileResponse, ForecastRunOnceProfileNodeRequest, ForecastRunOnceProfileNodeResponse> {
    private static final Logger LOG = LogManager.getLogger(ForecastRunOnceProfileTransportAction.class);
    private final ForecastColdStartWorker coldStartWorker;
    private final ForecastCheckpointReadWorker checkpointReadWorker;
    private final NodeStateManager nodeStateManager;

    /**
     * Constructor
     *
     * @param threadPool ThreadPool to use
     * @param clusterService ClusterService
     * @param transportService TransportService
     * @param actionFilters Action Filters
     * @param settings Node settings accessor
     * @param coldStartWorker cold start worker
     * @param checkpointReadWorker checkpoint read worker
     * @param nodeStateManager node state manager
     */
    @Inject
    public ForecastRunOnceProfileTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Settings settings,
        ForecastColdStartWorker coldStartWorker,
        ForecastCheckpointReadWorker checkpointReadWorker,
        NodeStateManager nodeStateManager
    ) {
        super(
            ForecastRunOnceProfileAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ForecastRunOnceProfileRequest::new,
            ForecastRunOnceProfileNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ForecastRunOnceProfileNodeResponse.class
        );
        this.coldStartWorker = coldStartWorker;
        this.checkpointReadWorker = checkpointReadWorker;
        this.nodeStateManager = nodeStateManager;
    }

    @Override
    protected ForecastRunOnceProfileResponse newResponse(
        ForecastRunOnceProfileRequest request,
        List<ForecastRunOnceProfileNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new ForecastRunOnceProfileResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ForecastRunOnceProfileNodeRequest newNodeRequest(ForecastRunOnceProfileRequest request) {
        return new ForecastRunOnceProfileNodeRequest(request);
    }

    @Override
    protected ForecastRunOnceProfileNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ForecastRunOnceProfileNodeResponse(in);
    }

    @Override
    protected ForecastRunOnceProfileNodeResponse nodeOperation(ForecastRunOnceProfileNodeRequest request) {
        String configId = request.getConfigId();
        Optional<Exception> exception = nodeStateManager.fetchExceptionAndClear(configId);

        return new ForecastRunOnceProfileNodeResponse(
            clusterService.localNode(),
            coldStartWorker.hasInflightRequest(configId)
                || checkpointReadWorker.hasInflightRequest(configId)
                || coldStartWorker.hasConfigIdInQueue(configId)
                || checkpointReadWorker.hasConfigIdInQueue(configId),
            exception.isEmpty() ? null : ExceptionUtil.getErrorMessage(exception.get())
        );
    }
}
