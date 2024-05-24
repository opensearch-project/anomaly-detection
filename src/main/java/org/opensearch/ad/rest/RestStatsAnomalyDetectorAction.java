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

import static org.opensearch.timeseries.TimeSeriesAnalyticsPlugin.AD_BASE_URI;
import static org.opensearch.timeseries.TimeSeriesAnalyticsPlugin.LEGACY_AD_BASE;

import java.util.List;

import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.transport.StatsAnomalyDetectorAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.rest.RestStatsAction;
import org.opensearch.timeseries.transport.StatsRequest;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

import com.google.common.collect.ImmutableList;

/**
 * RestStatsAnomalyDetectorAction consists of the REST handler to get the stats from AD.
 */
public class RestStatsAnomalyDetectorAction extends RestStatsAction {

    private static final String STATS_ANOMALY_DETECTOR_ACTION = "stats_anomaly_detector";

    /**
     * Constructor
     *
     * @param timeSeriesStats TimeSeriesStats object
     * @param nodeFilter util class to get eligible data nodes
     */
    public RestStatsAnomalyDetectorAction(ADStats timeSeriesStats, DiscoveryNodeFilterer nodeFilter) {
        super(timeSeriesStats, nodeFilter);
    }

    @Override
    public String getName() {
        return STATS_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if (!ADEnabledSetting.isADEnabled()) {
            throw new IllegalStateException(ADCommonMessages.DISABLED_ERR_MSG);
        }
        StatsRequest adStatsRequest = getRequest(request);
        return channel -> client.execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest, new RestToXContentListener<>(channel));
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
