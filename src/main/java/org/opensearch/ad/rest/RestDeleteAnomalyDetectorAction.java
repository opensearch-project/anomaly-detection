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

import static org.opensearch.ad.util.RestHandlerUtils.DETECTOR_ID;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.rest.handler.AnomalyDetectorActionHandler;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.DeleteAnomalyDetectorAction;
import org.opensearch.ad.transport.DeleteAnomalyDetectorRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.rest.BaseExtensionRestHandler;
import org.opensearch.sdk.rest.ReplacedRouteHandler;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to delete anomaly detector.
 */
public class RestDeleteAnomalyDetectorAction extends BaseExtensionRestHandler {

    public static final String DELETE_ANOMALY_DETECTOR_ACTION = "delete_anomaly_detector";

    private static final Logger logger = LogManager.getLogger(RestDeleteAnomalyDetectorAction.class);
    private final AnomalyDetectorActionHandler handler = new AnomalyDetectorActionHandler();

    private Settings settings;
    private SDKRestClient sdkRestClient;
    private ExtensionsRunner extensionsRunner;

    public RestDeleteAnomalyDetectorAction(ExtensionsRunner extensionsRunner, SDKRestClient sdkRestClient) {
        this.extensionsRunner = extensionsRunner;
        this.settings = extensionsRunner.getEnvironmentSettings();
        this.sdkRestClient = sdkRestClient;
    }

    public String getName() {
        return DELETE_ANOMALY_DETECTOR_ACTION;
    }

    protected ExtensionRestResponse prepareRequest(RestRequest request) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        String detectorId = request.param(DETECTOR_ID);
        DeleteAnomalyDetectorRequest deleteAnomalyDetectorRequest = new DeleteAnomalyDetectorRequest(detectorId);
        CompletableFuture<DeleteResponse> futureResponse = new CompletableFuture<>();

        sdkRestClient
            .execute(
                DeleteAnomalyDetectorAction.INSTANCE,
                deleteAnomalyDetectorRequest,
                ActionListener.wrap(r -> futureResponse.complete(r), e -> futureResponse.completeExceptionally(e))
            );

        DeleteResponse response = futureResponse
            .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings).getMillis(), TimeUnit.MILLISECONDS)
            .join();

        return deleteAnomalyDetectorResponse(request, response);
    }

    public List<ReplacedRouteHandler> replacedRouteHandlers() {
        return ImmutableList
            .of(
                // delete anomaly detector document
                new ReplacedRouteHandler(
                    RestRequest.Method.DELETE,
                    String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, DETECTOR_ID),
                    RestRequest.Method.DELETE,
                    String.format(Locale.ROOT, "%s/{%s}", AnomalyDetectorExtension.LEGACY_OPENDISTRO_AD_BASE_URI, DETECTOR_ID),
                    handleRequest
                )
            );
    }

    private Function<RestRequest, ExtensionRestResponse> handleRequest = (request) -> {
        try {
            return prepareRequest(request);
        } catch (Exception e) {
            // TODO: handle the AD-specific exceptions separately
            return exceptionalRequest(request, e);
        }
    };

    private ExtensionRestResponse deleteAnomalyDetectorResponse(RestRequest request, DeleteResponse response) throws IOException {
        RestStatus restStatus = RestStatus.OK;
        ExtensionRestResponse extensionRestResponse = new ExtensionRestResponse(
            request,
            restStatus,
            response.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)
        );
        return extensionRestResponse;
    }

}
