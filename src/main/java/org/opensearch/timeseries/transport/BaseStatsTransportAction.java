/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.util.MultiResponsesDelegateActionListener;
import org.opensearch.transport.TransportService;

public abstract class BaseStatsTransportAction extends HandledTransportAction<StatsRequest, StatsTimeSeriesResponse> {
    public final Logger logger = LogManager.getLogger(BaseStatsTransportAction.class);

    protected final Client client;
    protected final Stats stats;
    protected final ClusterService clusterService;

    public BaseStatsTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        Stats stats,
        ClusterService clusterService,
        String statsAction

    ) {
        super(statsAction, transportService, actionFilters, StatsRequest::new);
        this.client = client;
        this.stats = stats;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, StatsRequest request, ActionListener<StatsTimeSeriesResponse> actionListener) {
        ActionListener<StatsTimeSeriesResponse> listener = wrapRestActionListener(actionListener, CommonMessages.FAIL_TO_GET_STATS);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            getStats(client, listener, request);
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    /**
     * Make the 2 requests to get the node and cluster statistics
     *
     * @param client Client
     * @param listener Listener to send response
     * @param statsRequest Request containing stats to be retrieved
     */
    public void getStats(Client client, ActionListener<StatsTimeSeriesResponse> listener, StatsRequest statsRequest) {
        // Use MultiResponsesDelegateActionListener to execute 2 async requests and create the response once they finish
        MultiResponsesDelegateActionListener<StatsResponse> delegateListener = new MultiResponsesDelegateActionListener<>(
            getRestStatsListener(listener),
            2,
            "Unable to return Stats",
            false
        );

        getClusterStats(client, delegateListener, statsRequest);
        getNodeStats(client, delegateListener, statsRequest);
    }

    /**
     * Listener sends response once Node Stats and Cluster Stats are gathered
     *
     * @param listener Listener to send response
     * @return ActionListener for StatsResponse
     */
    public ActionListener<StatsResponse> getRestStatsListener(ActionListener<StatsTimeSeriesResponse> listener) {
        return ActionListener
            .wrap(
                statsResponse -> { listener.onResponse(new StatsTimeSeriesResponse(statsResponse)); },
                exception -> listener.onFailure(new OpenSearchStatusException(exception.getMessage(), RestStatus.INTERNAL_SERVER_ERROR))
            );
    }

    /**
     * Collect Cluster Stats into map to be retrieved
     *
     * @param statsRequest Request containing stats to be retrieved
     * @return Map containing Cluster Stats
     */
    protected Map<String, Object> getClusterStatsMap(StatsRequest statsRequest) {
        Map<String, Object> clusterStats = new HashMap<>();
        Set<String> statsToBeRetrieved = statsRequest.getStatsToBeRetrieved();
        stats
            .getClusterStats()
            .entrySet()
            .stream()
            .filter(s -> statsToBeRetrieved.contains(s.getKey()))
            .forEach(s -> clusterStats.put(s.getKey(), s.getValue().getValue()));
        return clusterStats;
    }

    protected abstract void getClusterStats(
        Client client,
        MultiResponsesDelegateActionListener<StatsResponse> listener,
        StatsRequest adStatsRequest
    );

    protected abstract void getNodeStats(
        Client client,
        MultiResponsesDelegateActionListener<StatsResponse> listener,
        StatsRequest adStatsRequest
    );
}
