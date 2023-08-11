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

import static org.opensearch.ad.util.RestHandlerUtils.PREVIEW;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

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
import org.opensearch.ad.transport.PreviewAnomalyDetectorAction;
import org.opensearch.ad.transport.PreviewAnomalyDetectorRequest;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import com.google.common.collect.ImmutableList;

public class RestPreviewAnomalyDetectorAction extends BaseRestHandler {

    public static final String PREVIEW_ANOMALY_DETECTOR_ACTION = "preview_anomaly_detector";

    private static final Logger logger = LogManager.getLogger(RestPreviewAnomalyDetectorAction.class);

    public RestPreviewAnomalyDetectorAction() {}

    @Override
    public String getName() {
        return PREVIEW_ANOMALY_DETECTOR_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, org.opensearch.client.node.NodeClient client) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        AnomalyDetectorExecutionInput input = getAnomalyDetectorExecutionInput(request);

        return channel -> {
            String rawPath = request.rawPath();
            String error = validateAdExecutionInput(input);
            if (StringUtils.isNotBlank(error)) {
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, error));
                return;
            }
            PreviewAnomalyDetectorRequest previewRequest = new PreviewAnomalyDetectorRequest(
                input.getDetector(),
                input.getDetectorId(),
                input.getPeriodStart(),
                input.getPeriodEnd()
            );
            client.execute(PreviewAnomalyDetectorAction.INSTANCE, previewRequest, new RestToXContentListener<>(channel));
        };
    }

    private AnomalyDetectorExecutionInput getAnomalyDetectorExecutionInput(RestRequest request) throws IOException {
        String detectorId = null;
        if (request.hasParam(RestHandlerUtils.DETECTOR_ID)) {
            detectorId = request.param(RestHandlerUtils.DETECTOR_ID);
        }

        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        AnomalyDetectorExecutionInput input = AnomalyDetectorExecutionInput.parse(parser, detectorId);
        return input;
    }

    private String validateAdExecutionInput(AnomalyDetectorExecutionInput input) {
        if (input.getPeriodStart() == null || input.getPeriodEnd() == null) {
            return "Must set both period start and end date with epoch of milliseconds";
        }
        if (!input.getPeriodStart().isBefore(input.getPeriodEnd())) {
            return "Period start date should be before end date";
        }
        if (Strings.isEmpty(input.getDetectorId()) && input.getDetector() == null) {
            return "Must set detector id or detector";
        }
        return null;
    }

    @Override
    public List<RestHandler.Route> routes() {
        return ImmutableList
            .of(
                // preview detector
                new Route(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/%s", AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI, PREVIEW)
                )
            );
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return ImmutableList
            .of(
                // Preview Detector
                new ReplacedRoute(
                    RestRequest.Method.POST,
                    String
                        .format(
                            Locale.ROOT,
                            "%s/{%s}/%s",
                            AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI,
                            RestHandlerUtils.DETECTOR_ID,
                            PREVIEW
                        ),
                    RestRequest.Method.POST,
                    String
                        .format(
                            Locale.ROOT,
                            "%s/{%s}/%s",
                            AnomalyDetectorPlugin.LEGACY_OPENDISTRO_AD_BASE_URI,
                            RestHandlerUtils.DETECTOR_ID,
                            PREVIEW
                        )
                )
            );
    }
}
