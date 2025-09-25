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

package org.opensearch.ad.rest;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_REQUEST_TIMEOUT;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.util.RestHandlerUtils.SUGGEST;
import static org.opensearch.timeseries.util.RestHandlerUtils.TYPE;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.transport.SuggestAnomalyDetectorParamAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.ValidationException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.transport.SuggestConfigParamRequest;
import org.opensearch.transport.client.node.NodeClient;
import org.owasp.encoder.Encode;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to suggest anomaly detector configurations.
 */
public class RestAnomalyDetectorSuggestAction extends BaseRestHandler {
    private static final String ANOMALY_DETECTOR_SUGGEST_ACTION = "anomaly_detector_suggest_action";

    private volatile TimeValue requestTimeout;

    public RestAnomalyDetectorSuggestAction(Settings settings, ClusterService clusterService) {
        this.requestTimeout = AD_REQUEST_TIMEOUT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_REQUEST_TIMEOUT, it -> requestTimeout = it);
    }

    @Override
    public String getName() {
        return ANOMALY_DETECTOR_SUGGEST_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/%s/{%s}", TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, SUGGEST, TYPE)
                )
            );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!ADEnabledSetting.isADEnabled()) {
            throw new IllegalStateException(ADCommonMessages.DISABLED_ERR_MSG);
        }

        try {
            XContentParser parser = request.contentParser();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            String typesStr = request.param(TYPE);

            AnomalyDetector config = parseConfig(parser);

            if (config != null) {
                return channel -> {
                    SuggestConfigParamRequest suggestAnomalyDetectorParamRequest = new SuggestConfigParamRequest(
                        AnalysisType.AD,
                        config,
                        typesStr,
                        requestTimeout
                    );
                    client
                        .execute(
                            SuggestAnomalyDetectorParamAction.INSTANCE,
                            suggestAnomalyDetectorParamRequest,
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

    private AnomalyDetector parseConfig(XContentParser parser) throws IOException {
        try {
            // use default detector interval in case of validation exception since it can be empty
            return AnomalyDetector.parse(parser, null, null, new TimeValue(1, TimeUnit.MINUTES), null);
        } catch (Exception e) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError(e.getMessage());
            throw validationException;
        }
    }
}
