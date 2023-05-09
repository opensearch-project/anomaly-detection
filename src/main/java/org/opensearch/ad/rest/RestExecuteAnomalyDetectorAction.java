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
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultRequest;
import org.opensearch.ad.transport.AnomalyResultResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
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

/**
 * This class consists of the REST handler to handle request to detect data.
 */
public class RestExecuteAnomalyDetectorAction extends BaseExtensionRestHandler {

    public static final String DETECT_DATA_ACTION = "execute_anomaly_detector";
    // TODO: apply timeout config
    private volatile TimeValue requestTimeout;
    private SDKRestClient sdkRestClient;
    private Settings settings;
    private final Logger logger = LogManager.getLogger(RestExecuteAnomalyDetectorAction.class);

    public RestExecuteAnomalyDetectorAction(ExtensionsRunner extensionsRunner, SDKRestClient sdkRestClient) {
        this.requestTimeout = REQUEST_TIMEOUT.get(extensionsRunner.getEnvironmentSettings());
        this.sdkRestClient = sdkRestClient;
        extensionsRunner.getSdkClusterService().getClusterSettings().addSettingsUpdateConsumer(REQUEST_TIMEOUT, it -> requestTimeout = it);
        this.settings = extensionsRunner.getEnvironmentSettings();
    }

    public String getName() {
        return DETECT_DATA_ACTION;
    }

    protected ExtensionRestResponse prepareRequest(RestRequest request) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        AnomalyDetectorExecutionInput input = getAnomalyDetectorExecutionInput(request);

        String rawPath = request.rawPath();
        String error = validateAdExecutionInput(input);

        if (StringUtils.isNotBlank(error)) {
            return getExecuteDetectorResponse(RestStatus.BAD_REQUEST, request, null, error);
        }

        CompletableFuture<AnomalyResultResponse> futureResponse = new CompletableFuture<>();

        AnomalyResultRequest anomalyResultRequest = new AnomalyResultRequest(
            input.getDetectorId(),
            input.getPeriodStart().toEpochMilli(),
            input.getPeriodEnd().toEpochMilli()
        );

        sdkRestClient
            .execute(
                AnomalyResultAction.INSTANCE,
                anomalyResultRequest,
                ActionListener.wrap(r -> futureResponse.complete(r), e -> futureResponse.completeExceptionally(e))
            );

        AnomalyResultResponse anomalyResultResponse = futureResponse
            .orTimeout(this.requestTimeout.getMillis(), TimeUnit.MILLISECONDS)
            .join();

        return getExecuteDetectorResponse(RestStatus.OK, request, anomalyResultResponse, anomalyResultResponse.toString());
    }

    private Function<RestRequest, ExtensionRestResponse> handleRequest = (request) -> {
        try {
            return prepareRequest(request);
        } catch (Exception e) {
            // TODO: handle the AD-specific exceptions separately
            return exceptionalRequest(request, e);
        }
    };

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
    public List<ReplacedRouteHandler> replacedRouteHandlers() {
        return ImmutableList
            .of(
                // get AD result, for regular run
                new ReplacedRouteHandler(
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, DETECTOR_ID, RUN),
                    RestRequest.Method.POST,
                    String.format(Locale.ROOT, "%s/{%s}/%s", AnomalyDetectorExtension.LEGACY_OPENDISTRO_AD_BASE_URI, DETECTOR_ID, RUN),
                    handleRequest
                )
            );
    }

    private ExtensionRestResponse getExecuteDetectorResponse(
        RestStatus restStatus,
        RestRequest request,
        AnomalyResultResponse response,
        String error
    ) throws IOException {
        ExtensionRestResponse extensionRestResponse;
        if (restStatus == RestStatus.OK) {
            extensionRestResponse = new ExtensionRestResponse(
                request,
                restStatus,
                response.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)
            );
        } else {
            extensionRestResponse = new ExtensionRestResponse(request, restStatus, error);
        }
        return extensionRestResponse;
    }
}
