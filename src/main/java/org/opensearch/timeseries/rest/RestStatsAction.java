/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.transport.StatsRequest;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

public abstract class RestStatsAction extends BaseRestHandler {
    private Stats timeSeriesStats;
    private DiscoveryNodeFilterer nodeFilter;

    /**
     * Constructor
     *
     * @param timeSeriesStats TimeSeriesStats object
     * @param nodeFilter util class to get eligible data nodes
     */
    public RestStatsAction(Stats timeSeriesStats, DiscoveryNodeFilterer nodeFilter) {
        this.timeSeriesStats = timeSeriesStats;
        this.nodeFilter = nodeFilter;
    }

    /**
     * Creates a StatsRequest from a RestRequest
     *
     * @param request RestRequest
     * @return StatsRequest Request containing stats to be retrieved
     */
    protected StatsRequest getRequest(RestRequest request) {
        // parse the nodes the user wants to query the stats for
        String nodesIdsStr = request.param("nodeId");
        Set<String> validStats = timeSeriesStats.getStats().keySet();

        StatsRequest statsRequest = null;
        if (!Strings.isEmpty(nodesIdsStr)) {
            String[] nodeIdsArr = nodesIdsStr.split(",");
            statsRequest = new StatsRequest(nodeIdsArr);
        } else {
            DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
            statsRequest = new StatsRequest(dataNodes);
        }

        statsRequest.timeout(request.param("timeout"));

        // parse the stats the user wants to see
        HashSet<String> statsSet = null;
        String statsStr = request.param("stat");
        if (!Strings.isEmpty(statsStr)) {
            statsSet = new HashSet<>(Arrays.asList(statsStr.split(",")));
        }

        if (statsSet == null) {
            statsRequest.addAll(validStats); // retrieve all stats if none are specified
        } else if (statsSet.size() == 1 && statsSet.contains(StatsRequest.ALL_STATS_KEY)) {
            statsRequest.addAll(validStats);
        } else if (statsSet.contains(StatsRequest.ALL_STATS_KEY)) {
            throw new IllegalArgumentException(
                "Request " + request.path() + " contains " + StatsRequest.ALL_STATS_KEY + " and individual stats"
            );
        } else {
            Set<String> invalidStats = new TreeSet<>();
            for (String stat : statsSet) {
                if (validStats.contains(stat)) {
                    statsRequest.addStat(stat);
                } else {
                    invalidStats.add(stat);
                }
            }

            if (!invalidStats.isEmpty()) {
                throw new IllegalArgumentException(unrecognized(request, invalidStats, statsRequest.getStatsToBeRetrieved(), "stat"));
            }
        }
        return statsRequest;
    }

}
