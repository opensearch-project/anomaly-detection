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

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.transport.SearchTopForecastResultAction;
import org.opensearch.forecast.transport.SearchTopForecastResultRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.client.node.NodeClient;
import org.owasp.encoder.Encode;

import com.google.common.collect.ImmutableList;

/**
 * The REST handler to search top entity anomaly results for HC detectors.
 */
public class RestSearchTopForecastResultAction extends BaseRestHandler {

    private static final String URL_PATH = String
        .format(
            Locale.ROOT,
            "%s/{%s}/%s/%s",
            TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI,
            RestHandlerUtils.FORECASTER_ID,
            RestHandlerUtils.RESULTS,
            RestHandlerUtils.TOP_FORECASTS
        );
    private final String SEARCH_TOP_FORECASTS_ACTION = "search_top_forecasts";

    public RestSearchTopForecastResultAction() {}

    @Override
    public String getName() {
        return SEARCH_TOP_FORECASTS_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        // Throw error if disabled
        if (!ForecastEnabledSetting.isForecastEnabled()) {
            throw new IllegalStateException(ForecastCommonMessages.DISABLED_ERR_MSG);
        }

        try {
            // Get the typed request
            SearchTopForecastResultRequest searchTopAnomalyResultRequest = getSearchTopForecastResultRequest(request);

            return channel -> client
                .execute(SearchTopForecastResultAction.INSTANCE, searchTopAnomalyResultRequest, new RestToXContentListener<>(channel));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(Encode.forHtml(e.getMessage()));
        }

    }

    private SearchTopForecastResultRequest getSearchTopForecastResultRequest(RestRequest request) throws IOException {
        String forecasterId;
        if (request.hasParam(RestHandlerUtils.FORECASTER_ID)) {
            forecasterId = request.param(RestHandlerUtils.FORECASTER_ID);
        } else {
            throw new IllegalStateException(ForecastCommonMessages.FORECASTER_ID_MISSING_MSG);
        }
        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        return SearchTopForecastResultRequest.parse(parser, forecasterId);
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(RestRequest.Method.POST, URL_PATH), new Route(RestRequest.Method.GET, URL_PATH));
    }
}
