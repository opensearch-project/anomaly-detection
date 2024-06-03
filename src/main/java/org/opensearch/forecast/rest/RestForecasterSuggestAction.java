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
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_REQUEST_TIMEOUT;
import static org.opensearch.timeseries.util.RestHandlerUtils.SUGGEST;
import static org.opensearch.timeseries.util.RestHandlerUtils.TYPE;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.ValidationException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.transport.SuggestForecasterParamAction;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.transport.SuggestConfigParamRequest;
import org.owasp.encoder.Encode;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to validate anomaly detector configurations.
 */
public class RestForecasterSuggestAction extends BaseRestHandler {
    private static final String FORECASTER_SUGGEST_ACTION = "forecaster_suggest_action";

    private volatile TimeValue requestTimeout;

    public RestForecasterSuggestAction(Settings settings, ClusterService clusterService) {
        this.requestTimeout = FORECAST_REQUEST_TIMEOUT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_REQUEST_TIMEOUT, it -> requestTimeout = it);
    }

    @Override
    public String getName() {
        return FORECASTER_SUGGEST_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/%s/{%s}", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, SUGGEST, TYPE)
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

            Forecaster config = parseConfig(parser);

            if (config != null) {
                return channel -> {
                    SuggestConfigParamRequest suggestForecasterParamRequest = new SuggestConfigParamRequest(
                        AnalysisType.FORECAST,
                        config,
                        typesStr,
                        requestTimeout
                    );
                    client
                        .execute(
                            SuggestForecasterParamAction.INSTANCE,
                            suggestForecasterParamRequest,
                            new RestToXContentListener<>(channel)
                        );
                };
            } else {
                ValidationException validationException = new ValidationException();
                validationException.addValidationError("fail to parse config");
                throw validationException;
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(Encode.forHtml(e.getMessage()));
        }
    }

    private Forecaster parseConfig(XContentParser parser) throws IOException {
        try {
            // use default forecaster interval in case of validation exception since it can be empty
            return Forecaster.parse(parser, null, null, new TimeValue(1, TimeUnit.MINUTES), null);
        } catch (Exception e) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError(e.getMessage());
            throw validationException;
        }
    }
}
