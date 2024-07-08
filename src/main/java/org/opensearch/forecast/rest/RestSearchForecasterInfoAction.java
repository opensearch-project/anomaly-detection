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

import static org.opensearch.timeseries.util.RestHandlerUtils.COUNT;
import static org.opensearch.timeseries.util.RestHandlerUtils.MATCH;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.transport.SearchForecasterInfoAction;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.transport.SearchConfigInfoRequest;
import org.owasp.encoder.Encode;

import com.google.common.collect.ImmutableList;

public class RestSearchForecasterInfoAction extends BaseRestHandler {

    public static final String SEARCH_FORECASTER_INFO_ACTION = "search_forecaster_info";

    public RestSearchForecasterInfoAction() {}

    @Override
    public String getName() {
        return SEARCH_FORECASTER_INFO_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, org.opensearch.client.node.NodeClient client) throws IOException {
        if (!ForecastEnabledSetting.isForecastEnabled()) {
            throw new IllegalStateException(ForecastCommonMessages.DISABLED_ERR_MSG);
        }

        try {
            String forecasterName = request.param("name", null);
            String rawPath = request.rawPath();

            SearchConfigInfoRequest searchForecasterInfoRequest = new SearchConfigInfoRequest(forecasterName, rawPath);
            return channel -> client
                .execute(SearchForecasterInfoAction.INSTANCE, searchForecasterInfoRequest, new RestToXContentListener<>(channel));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(Encode.forHtml(e.getMessage()));
        }
    }

    @Override
    public List<RestHandler.Route> routes() {
        return ImmutableList
            .of(
                new Route(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, COUNT)
                ),
                new Route(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, MATCH)
                )
            );
    }
}
