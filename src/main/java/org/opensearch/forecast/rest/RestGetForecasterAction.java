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
import static org.opensearch.timeseries.util.RestHandlerUtils.TYPE;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.transport.GetForecasterAction;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.transport.GetConfigRequest;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.client.node.NodeClient;
import org.owasp.encoder.Encode;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to retrieve an anomaly detector.
 */
public class RestGetForecasterAction extends BaseRestHandler {

    private static final String GET_FORECASTER_ACTION = "get_forecaster";

    public RestGetForecasterAction() {}

    @Override
    public String getName() {
        return GET_FORECASTER_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!ForecastEnabledSetting.isForecastEnabled()) {
            throw new IllegalStateException(ForecastCommonMessages.DISABLED_ERR_MSG);
        }

        try {
            String forecasterId = request.param(FORECASTER_ID);
            String typesStr = request.param(TYPE);

            String rawPath = request.rawPath();
            boolean returnJob = request.paramAsBoolean("job", false);
            boolean returnTask = request.paramAsBoolean("task", false);
            boolean all = request.paramAsBoolean("_all", false);
            GetConfigRequest getForecasterRequest = new GetConfigRequest(
                forecasterId,
                RestActions.parseVersion(request),
                returnJob,
                returnTask,
                typesStr,
                rawPath,
                all,
                RestHandlerUtils.buildEntity(request, forecasterId)
            );
            return channel -> client.execute(GetForecasterAction.INSTANCE, getForecasterRequest, new RestToXContentListener<>(channel));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(Encode.forHtml(e.getMessage()));
        }
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                // Opensearch-only API. Considering users may provide entity in the search body,
                // support POST as well.

                // profile API
                new Route(
                    RestRequest.Method.POST,
                    String
                        .format(
                            Locale.ROOT,
                            "%s/{%s}/%s",
                            TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI,
                            FORECASTER_ID,
                            RestHandlerUtils.PROFILE
                        )
                ),
                // types is a profile names. See a complete list of supported profiles names in
                // org.opensearch.ad.model.ProfileName.
                new Route(
                    RestRequest.Method.POST,
                    String
                        .format(
                            Locale.ROOT,
                            "%s/{%s}/%s/{%s}",
                            TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI,
                            FORECASTER_ID,
                            RestHandlerUtils.PROFILE,
                            TYPE
                        )
                ),
                new Route(
                    RestRequest.Method.GET,
                    String
                        .format(
                            Locale.ROOT,
                            "%s/{%s}/%s",
                            TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI,
                            FORECASTER_ID,
                            RestHandlerUtils.PROFILE
                        )
                ),
                // types is a profile names. See a complete list of supported profiles names in
                // org.opensearch.ad.model.ProfileName.
                new Route(
                    RestRequest.Method.GET,
                    String
                        .format(
                            Locale.ROOT,
                            "%s/{%s}/%s/{%s}",
                            TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI,
                            FORECASTER_ID,
                            RestHandlerUtils.PROFILE,
                            TYPE
                        )
                ),

                // get forecaster API
                new Route(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/{%s}", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, FORECASTER_ID)
                )
            );
    }
}
