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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;
import static org.opensearch.ad.util.RestHandlerUtils.DETECTOR_ID;
import static org.opensearch.ad.util.RestHandlerUtils.RUN;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyDetectorExecutionInput;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestToXContentListener;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to handle request to detect data.
 */
public class RestExecuteAnomalyDetectorAction extends BaseRestHandler {

    public static final String DETECT_DATA_ACTION = "execute_anomaly_detector";
    // TODO: apply timeout config
    private volatile TimeValue requestTimeout;

    private final Logger logger = LogManager.getLogger(RestExecuteAnomalyDetectorAction.class);

    public RestExecuteAnomalyDetectorAction(Settings settings, ClusterService clusterService) {
        this.requestTimeout = REQUEST_TIMEOUT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REQUEST_TIMEOUT, it -> requestTimeout = it);
    }

    @Override
    public String getName() {
        return DETECT_DATA_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        AnomalyDetectorExecutionInput input = getAnomalyDetectorExecutionInput(request);
        return channel -> {
            String error = validateAdExecutionInput(input);
            if (StringUtils.isNotBlank(error)) {
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, error));
                return;
            }

            AnomalyResultRequest getRequest = new AnomalyResultRequest(
                input.getDetectorId(),
                input.getPeriodStart().toEpochMilli(),
                input.getPeriodEnd().toEpochMilli()
            );
            client.execute(AnomalyResultAction.INSTANCE, getRequest, new RestToXContentListener<>(channel));
        };
    }

    private AnomalyDetectorExecutionInput getAnomalyDetectorExecutionInput(RestRequest request) throws IOException {
        String detectorId = null;
        if (request.hasParam(DETECTOR_ID)) {
            detectorId = request.param(DETECTOR_ID);
        }

        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        AnomalyDetectorExecutionInput input = AnomalyDetectorExecutionInput.parse(parser, detectorId);
        if (detectorId != null) {
            input.setDetectorId(detectorId);
        }
        return input;
    }

    private String validateAdExecutionInput(AnomalyDetectorExecutionInput input) {
        if (StringUtils.isBlank(input.getDetectorId()) && input.getDetector() == null) {
            return "Must set anomaly detector id or detector";
        }
        if (input.getPeriodStart() == null || input.getPeriodEnd() == null) {
            return "Must set both period start and end date with epoch of milliseconds";
        }
        if (!input.getPeriodStart().isBefore(input.getPeriodEnd())) {
            return "Period start date should be before end date";
        }
        return null;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return ImmutableList
            .of(
                // get AD result, for regular run
                new ReplacedRoute(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, DETECTOR_ID, RUN),
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorPlugin.LEGACY_OPENDISTRO_AD_BASE_URI, DETECTOR_ID, RUN)
                )
            );
    }
}
