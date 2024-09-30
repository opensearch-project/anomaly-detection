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
import static org.opensearch.timeseries.util.RestHandlerUtils.FORECASTER_ID;
import static org.opensearch.timeseries.util.RestHandlerUtils.IF_PRIMARY_TERM;
import static org.opensearch.timeseries.util.RestHandlerUtils.IF_SEQ_NO;
import static org.opensearch.timeseries.util.RestHandlerUtils.REFRESH;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.transport.IndexForecasterAction;
import org.opensearch.forecast.transport.IndexForecasterRequest;
import org.opensearch.forecast.transport.IndexForecasterResponse;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.model.Config;
import org.owasp.encoder.Encode;

import com.google.common.collect.ImmutableList;

/**
 * Rest handlers to create and update forecaster.
 */
public class RestIndexForecasterAction extends AbstractForecasterAction {
    private static final String INDEX_FORECASTER_ACTION = "index_forecaster_action";
    private final Logger logger = LogManager.getLogger(RestIndexForecasterAction.class);

    public RestIndexForecasterAction(Settings settings, ClusterService clusterService) {
        super(settings, clusterService);
    }

    @Override
    public String getName() {
        return INDEX_FORECASTER_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!ForecastEnabledSetting.isForecastEnabled()) {
            throw new IllegalStateException(ForecastCommonMessages.DISABLED_ERR_MSG);
        }

        try {
            String forecasterId = request.param(FORECASTER_ID, Config.NO_ID);
            logger.info("Forecaster {} action for forecasterId {}", request.method(), forecasterId);

            XContentParser parser = request.contentParser();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            Forecaster forecaster = Forecaster.parse(parser, forecasterId, null, forecastInterval, forecastWindowDelay);

            long seqNo = request.paramAsLong(IF_SEQ_NO, SequenceNumbers.UNASSIGNED_SEQ_NO);
            long primaryTerm = request.paramAsLong(IF_PRIMARY_TERM, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
            WriteRequest.RefreshPolicy refreshPolicy = request.hasParam(REFRESH)
                ? WriteRequest.RefreshPolicy.parse(request.param(REFRESH))
                : WriteRequest.RefreshPolicy.IMMEDIATE;
            RestRequest.Method method = request.getHttpRequest().method();

            if (method == RestRequest.Method.POST && forecasterId != Config.NO_ID) {
                // reset detector to empty string detectorId is only meant for updating detector
                forecasterId = Config.NO_ID;
            }

            IndexForecasterRequest indexAnomalyDetectorRequest = new IndexForecasterRequest(
                forecasterId,
                seqNo,
                primaryTerm,
                refreshPolicy,
                forecaster,
                method,
                requestTimeout,
                maxSingleStreamForecasters,
                maxHCForecasters,
                maxForecastFeatures,
                maxCategoricalFields
            );

            return channel -> client
                .execute(IndexForecasterAction.INSTANCE, indexAnomalyDetectorRequest, indexForecasterResponse(channel, method));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(Encode.forHtml(e.getMessage()));
        } catch (ValidationException e) {
            // convert 500 to 400 errors for validation failures
            throw new OpenSearchStatusException(e.getMessage(), RestStatus.BAD_REQUEST);
        }
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                // Create
                new Route(RestRequest.Method.POST, TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI),
                // Update
                new Route(
                    RestRequest.Method.PUT,
                    String.format(Locale.ROOT, "%s/{%s}", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, FORECASTER_ID)
                )
            );
    }

    private RestResponseListener<IndexForecasterResponse> indexForecasterResponse(RestChannel channel, RestRequest.Method method) {
        return new RestResponseListener<IndexForecasterResponse>(channel) {
            @Override
            public RestResponse buildResponse(IndexForecasterResponse response) throws Exception {
                RestStatus restStatus = RestStatus.CREATED;
                if (method == RestRequest.Method.PUT) {
                    restStatus = RestStatus.OK;
                }
                BytesRestResponse bytesRestResponse = new BytesRestResponse(
                    restStatus,
                    response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS)
                );
                if (restStatus == RestStatus.CREATED) {
                    String location = String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_BASE_URI, response.getId());
                    bytesRestResponse.addHeader("Location", location);
                }
                return bytesRestResponse;
            }
        };
    }
}
