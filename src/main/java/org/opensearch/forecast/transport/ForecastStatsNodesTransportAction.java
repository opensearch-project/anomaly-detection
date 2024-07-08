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

package org.opensearch.forecast.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.forecast.stats.ForecastStats;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.transport.BaseStatsNodesTransportAction;
import org.opensearch.transport.TransportService;

/**
 *  ForecastStatsNodesTransportAction contains the logic to extract the stats from the nodes
 */
public class ForecastStatsNodesTransportAction extends BaseStatsNodesTransportAction {
    @Inject
    public ForecastStatsNodesTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ForecastStats stats
    ) {
        super(threadPool, clusterService, transportService, actionFilters, stats, ForecastStatsNodesAction.NAME);
    }
}
