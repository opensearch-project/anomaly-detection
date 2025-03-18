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
import static org.opensearch.timeseries.util.RestHandlerUtils.TYPE;
import static org.opensearch.timeseries.util.RestHandlerUtils.VALIDATE;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.transport.ValidateForecasterAction;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.model.ConfigValidationIssue;
import org.opensearch.timeseries.rest.RestValidateAction;
import org.opensearch.timeseries.transport.ValidateConfigRequest;
import org.opensearch.transport.client.node.NodeClient;
import org.owasp.encoder.Encode;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to validate anomaly detector configurations.
 */
public class RestValidateForecasterAction extends AbstractForecasterAction {
    private static final String VALIDATE_FORECASTER_ACTION = "validate_forecaster_action";

    private RestValidateAction validateAction;

    public RestValidateForecasterAction(Settings settings, ClusterService clusterService) {
        super(settings, clusterService);
        this.validateAction = new RestValidateAction(
            AnalysisType.FORECAST,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxForecastFeatures,
            maxCategoricalFields,
            requestTimeout
        );
    }

    @Override
    public String getName() {
        return VALIDATE_FORECASTER_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, VALIDATE)
                ),
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/%s/{%s}", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, VALIDATE, TYPE)
                )
            );
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!ForecastEnabledSetting.isForecastEnabled()) {
            throw new IllegalStateException(ForecastCommonMessages.DISABLED_ERR_MSG);
        }

        try {
            XContentParser parser = request.contentParser();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            // we have to get the param from a subclass of BaseRestHandler. Otherwise, we cannot parse the type out of request params
            String typesStr = request.param(TYPE);

            return channel -> {
                try {
                    ValidateConfigRequest validateForecasterRequest = validateAction.prepareRequest(request, client, typesStr);
                    client.execute(ValidateForecasterAction.INSTANCE, validateForecasterRequest, new RestToXContentListener<>(channel));
                } catch (Exception ex) {
                    if (ex instanceof ValidationException) {
                        ValidationException forecastException = (ValidationException) ex;
                        ConfigValidationIssue issue = new ConfigValidationIssue(
                            forecastException.getAspect(),
                            forecastException.getType(),
                            forecastException.getMessage()
                        );
                        validateAction.sendValidationParseResponse(issue, channel);
                    } else {
                        throw ex;
                    }
                }
            };
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(Encode.forHtml(e.getMessage()));
        }
    }
}
