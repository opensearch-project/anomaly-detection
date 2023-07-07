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

package org.opensearch.ad.rest;

import static org.opensearch.ad.AnomalyDetectorPlugin.AD_BASE_URI;
import static org.opensearch.ad.AnomalyDetectorPlugin.LEGACY_AD_BASE;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.transport.ADStatsRequest;
import org.opensearch.ad.transport.StatsAnomalyDetectorAction;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import com.google.common.collect.ImmutableList;

/**
 * RestStatsAnomalyDetectorAction consists of the REST handler to get the stats from the anomaly detector plugin.
 */
public class RestStatsAnomalyDetectorAction extends BaseRestHandler {

    private static final String STATS_ANOMALY_DETECTOR_ACTION = "stats_anomaly_detector";
    private ADStats adStats;
    private ClusterService clusterService;
    private DiscoveryNodeFilterer nodeFilter;

    /**
     * Constructor
     *
     * @param adStats ADStats object
     * @param nodeFilter util class to get eligible data nodes
     */
    public RestStatsAnomalyDetectorAction(ADStats adStats, DiscoveryNodeFilterer nodeFilter) {
        this.adStats = adStats;
        this.nodeFilter = nodeFilter;
    }

    @Override
    public String getName() {
        return STATS_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        ADStatsRequest adStatsRequest = getRequest(request);
        return channel -> client.execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest, new RestToXContentListener<>(channel));
    }

    /**
     * Creates a ADStatsRequest from a RestRequest
     *
     * @param request RestRequest
     * @return ADStatsRequest Request containing stats to be retrieved
     */
    private ADStatsRequest getRequest(RestRequest request) {
        // parse the nodes the user wants to query the stats for
        String nodesIdsStr = request.param("nodeId");
        Set<String> validStats = adStats.getStats().keySet();

        ADStatsRequest adStatsRequest = null;
        if (!Strings.isEmpty(nodesIdsStr)) {
            String[] nodeIdsArr = nodesIdsStr.split(",");
            adStatsRequest = new ADStatsRequest(nodeIdsArr);
        } else {
            DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
            adStatsRequest = new ADStatsRequest(dataNodes);
        }

        adStatsRequest.timeout(request.param("timeout"));

        // parse the stats the user wants to see
        HashSet<String> statsSet = null;
        String statsStr = request.param("stat");
        if (!Strings.isEmpty(statsStr)) {
            statsSet = new HashSet<>(Arrays.asList(statsStr.split(",")));
        }

        if (statsSet == null) {
            adStatsRequest.addAll(validStats); // retrieve all stats if none are specified
        } else if (statsSet.size() == 1 && statsSet.contains(ADStatsRequest.ALL_STATS_KEY)) {
            adStatsRequest.addAll(validStats);
        } else if (statsSet.contains(ADStatsRequest.ALL_STATS_KEY)) {
            throw new IllegalArgumentException(
                "Request " + request.path() + " contains " + ADStatsRequest.ALL_STATS_KEY + " and individual stats"
            );
        } else {
            Set<String> invalidStats = new TreeSet<>();
            for (String stat : statsSet) {
                if (validStats.contains(stat)) {
                    adStatsRequest.addStat(stat);
                } else {
                    invalidStats.add(stat);
                }
            }

            if (!invalidStats.isEmpty()) {
                throw new IllegalArgumentException(unrecognized(request, invalidStats, adStatsRequest.getStatsToBeRetrieved(), "stat"));
            }
        }
        return adStatsRequest;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return ImmutableList
            .of(
                // delete anomaly detector document
                new ReplacedRoute(
                    RestRequest.Method.GET,
                    AD_BASE_URI + "/{nodeId}/stats/",
                    RestRequest.Method.GET,
                    LEGACY_AD_BASE + "/{nodeId}/stats/"
                ),
                new ReplacedRoute(
                    RestRequest.Method.GET,
                    AD_BASE_URI + "/{nodeId}/stats/{stat}",
                    RestRequest.Method.GET,
                    LEGACY_AD_BASE + "/{nodeId}/stats/{stat}"
                ),
                new ReplacedRoute(RestRequest.Method.GET, AD_BASE_URI + "/stats/", RestRequest.Method.GET, LEGACY_AD_BASE + "/stats/"),
                new ReplacedRoute(
                    RestRequest.Method.GET,
                    AD_BASE_URI + "/stats/{stat}",
                    RestRequest.Method.GET,
                    LEGACY_AD_BASE + "/stats/{stat}"
                )
            );
    }
}
