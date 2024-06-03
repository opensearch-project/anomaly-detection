/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.transport.TransportService;

public class BaseStatsNodesTransportAction extends
    TransportNodesAction<StatsRequest, StatsNodesResponse, StatsNodeRequest, StatsNodeResponse> {

    private Stats stats;

    /**
     * Constructor
     *
     * @param threadPool       ThreadPool to use
     * @param clusterService   ClusterService
     * @param transportService TransportService
     * @param actionFilters    Action Filters
     * @param stats            TimeSeriesStats object
     */
    public BaseStatsNodesTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Stats stats,
        String statsNodesActionName
    ) {
        super(
            statsNodesActionName,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            StatsRequest::new,
            StatsNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            StatsNodeResponse.class
        );
        this.stats = stats;
    }

    @Override
    protected StatsNodesResponse newResponse(StatsRequest request, List<StatsNodeResponse> responses, List<FailedNodeException> failures) {
        return new StatsNodesResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected StatsNodeRequest newNodeRequest(StatsRequest request) {
        return new StatsNodeRequest(request);
    }

    @Override
    protected StatsNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new StatsNodeResponse(in);
    }

    @Override
    protected StatsNodeResponse nodeOperation(StatsNodeRequest request) {
        return createADStatsNodeResponse(request.getADStatsRequest());
    }

    protected StatsNodeResponse createADStatsNodeResponse(StatsRequest statsRequest) {
        Map<String, Object> statValues = new HashMap<>();
        Set<String> statsToBeRetrieved = statsRequest.getStatsToBeRetrieved();

        for (String statName : stats.getNodeStats().keySet()) {
            if (statsToBeRetrieved.contains(statName)) {
                statValues.put(statName, stats.getStats().get(statName).getValue());
            }
        }

        return new StatsNodeResponse(clusterService.localNode(), statValues);
    }

}
