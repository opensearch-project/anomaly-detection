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
import static org.opensearch.timeseries.util.RestHandlerUtils.PREVIEW;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.transport.PreviewForecasterAction;
import org.opensearch.forecast.transport.PreviewForecasterRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.ImmutableList;

/**
 * REST handler for forecasting preview endpoint.
 */
public class RestPreviewForecasterAction extends BaseRestHandler {

    public static final String PREVIEW_FORECASTER_ACTION = "preview_forecaster";

    @Override
    public String getName() {
        return PREVIEW_FORECASTER_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, PREVIEW)
                )
            );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if (!ForecastEnabledSetting.isForecastEnabled()) {
            throw new IllegalStateException(ForecastCommonMessages.DISABLED_ERR_MSG);
        }

        ForecasterExecutionInput input;
        try {
            input = getForecasterExecutionInput(request);
        } catch (IOException e) {
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, e.getMessage()));
        }

        String error = validateForecasterExecutionInput(input);
        if (error != null) {
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, error));
        }

        PreviewForecasterRequest previewRequest = new PreviewForecasterRequest(
            input.getForecaster(),
            input.getForecasterId(),
            input.getPeriodStart(),
            input.getPeriodEnd()
        );

        return channel -> client.execute(PreviewForecasterAction.INSTANCE, previewRequest, new RestToXContentListener<>(channel));
    }

    private ForecasterExecutionInput getForecasterExecutionInput(RestRequest request) throws IOException {
        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        return ForecasterExecutionInput.parse(parser);
    }

    private String validateForecasterExecutionInput(ForecasterExecutionInput input) {
        if (input.getPeriodStart() == null || input.getPeriodEnd() == null) {
            return "Must set both period start and end date with epoch of milliseconds";
        }
        if (!input.getPeriodStart().isBefore(input.getPeriodEnd())) {
            return "Period start date should be before end date";
        }
        if (input.getForecasterId() == null && input.getForecaster() == null) {
            return "Must set forecaster id or forecaster";
        }
        return null;
    }
}
