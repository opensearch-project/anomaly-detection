/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import java.io.IOException;
import java.util.List;

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
import org.opensearch.timeseries.transport.BooleanNodeResponse;
import org.opensearch.timeseries.transport.BooleanResponse;
import org.opensearch.transport.TransportService;

public class ForecastRunOnceProfileTransportAction extends
    TransportNodesAction<ForecastRunOnceProfileRequest, BooleanResponse, ForecastRunOnceProfileNodeRequest, BooleanNodeResponse> {
    private final ForecastColdStartWorker coldStartWorker;
    private final ForecastCheckpointReadWorker checkpointReadWorker;

    /**
     * Constructor
     *
     * @param threadPool ThreadPool to use
     * @param clusterService ClusterService
     * @param transportService TransportService
     * @param actionFilters Action Filters
     * @param settings Node settings accessor
     */
    @Inject
    public ForecastRunOnceProfileTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Settings settings,
        ForecastColdStartWorker coldStartWorker,
        ForecastCheckpointReadWorker checkpointReadWorker
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
            BooleanNodeResponse.class
        );
        this.coldStartWorker = coldStartWorker;
        this.checkpointReadWorker = checkpointReadWorker;
    }

    @Override
    protected BooleanResponse newResponse(
        ForecastRunOnceProfileRequest request,
        List<BooleanNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new BooleanResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ForecastRunOnceProfileNodeRequest newNodeRequest(ForecastRunOnceProfileRequest request) {
        return new ForecastRunOnceProfileNodeRequest(request);
    }

    @Override
    protected BooleanNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new BooleanNodeResponse(in);
    }

    @Override
    protected BooleanNodeResponse nodeOperation(ForecastRunOnceProfileNodeRequest request) {
        String configId = request.getConfigId();

        return new BooleanNodeResponse(
            clusterService.localNode(),
            coldStartWorker.hasConfigId(configId) || checkpointReadWorker.hasConfigId(configId)
        );
    }
}
