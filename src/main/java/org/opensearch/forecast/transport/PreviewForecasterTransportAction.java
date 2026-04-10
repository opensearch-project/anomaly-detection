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

package org.opensearch.forecast.transport;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.util.RestHandlerUtils.createXContentParserFromRegistry;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.ForecasterRunner;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class PreviewForecasterTransportAction extends HandledTransportAction<PreviewForecasterRequest, PreviewForecasterResponse> {

    public static final String MISSING_REQUEST_MESSAGE = "Must set forecaster id or forecaster";
    private final ForecasterRunner forecasterRunner;
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public PreviewForecasterTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ForecasterRunner forecasterRunner,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(PreviewForecasterAction.NAME, transportService, actionFilters, PreviewForecasterRequest::new);
        this.forecasterRunner = forecasterRunner;
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected void doExecute(Task task, PreviewForecasterRequest request, ActionListener<PreviewForecasterResponse> listener) {
        Forecaster forecaster = request.getForecaster();
        if (forecaster != null) {
            executePreview(forecaster, request, listener);
            return;
        }

        String forecasterId = request.getForecasterId();
        if (StringUtils.isBlank(forecasterId)) {
            listener.onFailure(new OpenSearchStatusException(MISSING_REQUEST_MESSAGE, RestStatus.BAD_REQUEST));
            return;
        }

        GetRequest getRequest = new GetRequest(ForecastCommonName.CONFIG_INDEX).id(forecasterId);
        client.get(getRequest, onGetForecasterResponse(request, listener));
    }

    private void executePreview(
        Forecaster forecaster,
        PreviewForecasterRequest request,
        ActionListener<PreviewForecasterResponse> listener
    ) {
        try {
            forecasterRunner
                .executeForecaster(
                    forecaster,
                    request.getPeriodStart(),
                    request.getPeriodEnd(),
                    ActionListener
                        .wrap(
                            forecastResult -> listener.onResponse(new PreviewForecasterResponse(forecastResult, forecaster)),
                            listener::onFailure
                        )
                );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private ActionListener<GetResponse> onGetForecasterResponse(
        PreviewForecasterRequest request,
        ActionListener<PreviewForecasterResponse> listener
    ) {
        return ActionListener.wrap(response -> {
            if (!response.isExists()) {
                listener
                    .onFailure(new OpenSearchStatusException("Can't find forecaster with id:" + response.getId(), RestStatus.NOT_FOUND));
                return;
            }

            try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                Forecaster forecaster = Forecaster.parse(parser, response.getId(), response.getVersion());
                executePreview(forecaster, request, listener);
            } catch (IOException e) {
                listener.onFailure(e);
            }
        }, e -> listener.onFailure(normalizeGetFailure(e, request.getForecasterId())));
    }

    private Exception normalizeGetFailure(Exception exception, String forecasterId) {
        if (exception instanceof IndexNotFoundException) {
            return new OpenSearchStatusException("Can't find forecaster with id:" + forecasterId, RestStatus.NOT_FOUND, exception);
        }
        return exception;
    }
}
