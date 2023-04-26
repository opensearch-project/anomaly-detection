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
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyDetectorExecutionInput;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.PreviewAnomalyDetectorAction;
import org.opensearch.ad.transport.PreviewAnomalyDetectorRequest;
import org.opensearch.ad.transport.PreviewAnomalyDetectorResponse;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.rest.BaseExtensionRestHandler;
import org.opensearch.sdk.rest.ReplacedRouteHandler;

import com.google.common.collect.ImmutableList;

public class RestPreviewAnomalyDetectorAction extends BaseExtensionRestHandler {

    public static final String PREVIEW_ANOMALY_DETECTOR_ACTION = "preview_anomaly_detector";

    private static final Logger logger = LogManager.getLogger(RestPreviewAnomalyDetectorAction.class);

    private Settings environmentSettings;
    private SDKRestClient client;

    public RestPreviewAnomalyDetectorAction(ExtensionsRunner extensionsRunner, SDKRestClient sdkRestClient) {
        this.environmentSettings = extensionsRunner.getEnvironmentSettings();
        this.client = sdkRestClient;
    }

    public String getName() {
        return PREVIEW_ANOMALY_DETECTOR_ACTION;
    }

    private Function<RestRequest, ExtensionRestResponse> handleRequest = (request) -> {
        try {
            return prepareRequest(request);
        } catch (Exception e) {
            // TODO: handle the AD-specific exceptions separately
            return exceptionalRequest(request, e);
        }
    };

    protected ExtensionRestResponse prepareRequest(RestRequest request) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        AnomalyDetectorExecutionInput input = getAnomalyDetectorExecutionInput(request);

        // String rawPath = request.rawPath();
        String error = validateAdExecutionInput(input);
        if (StringUtils.isNotBlank(error)) {
            return new ExtensionRestResponse(request, RestStatus.BAD_REQUEST, error);
        }
        PreviewAnomalyDetectorRequest previewRequest = new PreviewAnomalyDetectorRequest(
            input.getDetector(),
            input.getDetectorId(),
            input.getPeriodStart(),
            input.getPeriodEnd()
        );
        CompletableFuture<PreviewAnomalyDetectorResponse> futureResponse = new CompletableFuture<>();
        client
            .execute(
                PreviewAnomalyDetectorAction.INSTANCE,
                previewRequest,
                ActionListener.wrap(r -> futureResponse.complete(r), e -> futureResponse.completeExceptionally(e))
            );
        PreviewAnomalyDetectorResponse response = futureResponse
            .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(environmentSettings).getMillis(), TimeUnit.MILLISECONDS)
            .join();
        // TODO handle exceptional response
        return new ExtensionRestResponse(
            request,
            RestStatus.OK,
            response.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)
        );
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
    public List<RouteHandler> routeHandlers() {
        return ImmutableList
            .of(
                // preview detector
                new RouteHandler(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/%s", AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, PREVIEW),
                    handleRequest
                )
            );
    }

    @Override
    public List<ReplacedRouteHandler> replacedRouteHandlers() {
        return ImmutableList
            .of(
                // Preview Detector
                new ReplacedRouteHandler(
                    RestRequest.Method.POST,
                    String
                        .format(
                            Locale.ROOT,
                            "%s/{%s}/%s",
                            AnomalyDetectorExtension.AD_BASE_DETECTORS_URI,
                            RestHandlerUtils.DETECTOR_ID,
                            PREVIEW
                        ),
                    RestRequest.Method.POST,
                    String
                        .format(
                            Locale.ROOT,
                            "%s/{%s}/%s",
                            AnomalyDetectorExtension.LEGACY_OPENDISTRO_AD_BASE_URI,
                            RestHandlerUtils.DETECTOR_ID,
                            PREVIEW
                        ),
                    handleRequest
                )
            );
    }
}
