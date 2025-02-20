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

package org.opensearch.forecast.rest;

import java.util.List;

import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.stats.ForecastStats;
import org.opensearch.forecast.transport.StatsForecasterAction;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.rest.RestStatsAction;
import org.opensearch.timeseries.transport.StatsRequest;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.client.node.NodeClient;
import org.owasp.encoder.Encode;

import com.google.common.collect.ImmutableList;

/**
 * RestStatsForecasterAction consists of the REST handler to get the stats from forecasting.
 */
public class RestStatsForecasterAction extends RestStatsAction {

    private static final String STATS_FORECASTER_ACTION = "stats_forecaster";

    /**
     * Constructor
     *
     * @param timeSeriesStats TimeSeriesStats object
     * @param nodeFilter util class to get eligible data nodes
     */
    public RestStatsForecasterAction(ForecastStats timeSeriesStats, DiscoveryNodeFilterer nodeFilter) {
        super(timeSeriesStats, nodeFilter);
    }

    @Override
    public String getName() {
        return STATS_FORECASTER_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if (!ForecastEnabledSetting.isForecastEnabled()) {
            throw new IllegalStateException(ForecastCommonMessages.DISABLED_ERR_MSG);
        }

        try {
            StatsRequest forecastStatsRequest = getRequest(request);
            return channel -> client.execute(StatsForecasterAction.INSTANCE, forecastStatsRequest, new RestToXContentListener<>(channel));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(Encode.forHtml(e.getMessage()));
        }
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                new Route(RestRequest.Method.GET, TimeSeriesAnalyticsPlugin.FORECAST_BASE_URI + "/{nodeId}/stats/"),
                new Route(RestRequest.Method.GET, TimeSeriesAnalyticsPlugin.FORECAST_BASE_URI + "/{nodeId}/stats/{stat}"),
                new Route(RestRequest.Method.GET, TimeSeriesAnalyticsPlugin.FORECAST_BASE_URI + "/stats/"),
                new Route(RestRequest.Method.GET, TimeSeriesAnalyticsPlugin.FORECAST_BASE_URI + "/stats/{stat}")
            );
    }
}
