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

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.util.RestHandlerUtils.TYPE;
import static org.opensearch.timeseries.util.RestHandlerUtils.VALIDATE;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.transport.ValidateAnomalyDetectorAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.model.ConfigValidationIssue;
import org.opensearch.timeseries.rest.RestValidateAction;
import org.opensearch.timeseries.transport.ValidateConfigRequest;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to validate anomaly detector configurations.
 */
public class RestValidateAnomalyDetectorAction extends AbstractAnomalyDetectorAction {
    private static final String VALIDATE_ANOMALY_DETECTOR_ACTION = "validate_anomaly_detector_action";

    private RestValidateAction validateAction;

    public RestValidateAnomalyDetectorAction(Settings settings, ClusterService clusterService) {
        super(settings, clusterService);
        this.validateAction = new RestValidateAction(
            AnalysisType.AD,
            maxSingleEntityDetectors,
            maxMultiEntityDetectors,
            maxAnomalyFeatures,
            maxCategoricalFields,
            requestTimeout
        );
    }

    @Override
    public String getName() {
        return VALIDATE_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
            .of(
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, VALIDATE)
                ),
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/%s/{%s}", TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, VALIDATE, TYPE)
                )
            );
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!ADEnabledSetting.isADEnabled()) {
            throw new IllegalStateException(ADCommonMessages.DISABLED_ERR_MSG);
        }

        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        // we have to get the param from a subclass of BaseRestHandler. Otherwise, we cannot parse the type out of request params
        String typesStr = request.param(TYPE);

        return channel -> {
            try {
                ValidateConfigRequest validateAnomalyDetectorRequest = validateAction.prepareRequest(request, client, typesStr);
                client
                    .execute(ValidateAnomalyDetectorAction.INSTANCE, validateAnomalyDetectorRequest, new RestToXContentListener<>(channel));
            } catch (Exception ex) {
                if (ex instanceof ValidationException) {
                    ValidationException adException = (ValidationException) ex;
                    ConfigValidationIssue issue = new ConfigValidationIssue(
                        adException.getAspect(),
                        adException.getType(),
                        adException.getMessage()
                    );
                    validateAction.sendValidationParseResponse(issue, channel);
                } else {
                    throw ex;
                }
            }
        };
    }
}
