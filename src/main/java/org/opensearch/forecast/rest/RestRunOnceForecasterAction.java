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

import static org.opensearch.timeseries.util.RestHandlerUtils.FORECASTER_ID;
import static org.opensearch.timeseries.util.RestHandlerUtils.RUN_ONCE;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.joda.time.Instant;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.transport.ForecastResultRequest;
import org.opensearch.forecast.transport.ForecastRunOnceAction;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.transport.client.node.NodeClient;
import org.owasp.encoder.Encode;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to handle request to forecast.
 */
public class RestRunOnceForecasterAction extends BaseRestHandler {

    public static final String FORECASTER_ACTION = "run_forecaster_once";

    public RestRunOnceForecasterAction() {}

    @Override
    public String getName() {
        return FORECASTER_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                // execute forester once
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, FORECASTER_ID, RUN_ONCE)
                )
            );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!ForecastEnabledSetting.isForecastEnabled()) {
            throw new IllegalStateException(ForecastCommonMessages.DISABLED_ERR_MSG);
        }

        try {
            String forecasterId = request.param(FORECASTER_ID);

            ForecastResultRequest getRequest = new ForecastResultRequest(
                forecasterId,
                -1L, // will set it in ResultProcessor.onGetConfig
                Instant.now().getMillis()
            );

            return channel -> client.execute(ForecastRunOnceAction.INSTANCE, getRequest, new RestToXContentListener<>(channel));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(Encode.forHtml(e.getMessage()));
        }
    }
}
