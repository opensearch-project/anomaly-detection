/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest;

import static org.opensearch.timeseries.util.RestHandlerUtils.FORECASTER_ID;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.transport.DeleteForecasterAction;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.transport.DeleteConfigRequest;
import org.opensearch.transport.client.node.NodeClient;
import org.owasp.encoder.Encode;

import com.google.common.collect.ImmutableList;

public class RestDeleteForecasterAction extends BaseRestHandler {
    public static final String DELETE_FORECASTER_ACTION = "delete_forecaster";

    public RestDeleteForecasterAction() {}

    @Override
    public String getName() {
        return DELETE_FORECASTER_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!ForecastEnabledSetting.isForecastEnabled()) {
            throw new IllegalStateException(ForecastCommonMessages.DISABLED_ERR_MSG);
        }

        try {
            String forecasterId = request.param(FORECASTER_ID);
            DeleteConfigRequest deleteForecasterRequest = new DeleteConfigRequest(forecasterId, ForecastIndex.CONFIG.getIndexName());
            return channel -> client
                .execute(DeleteForecasterAction.INSTANCE, deleteForecasterRequest, new RestToXContentListener<>(channel));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(Encode.forHtml(e.getMessage()));
        }

    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                // delete forecaster document
                new Route(
                    RestRequest.Method.DELETE,
                    String.format(Locale.ROOT, "%s/{%s}", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, FORECASTER_ID)
                )
            );
    }
}
