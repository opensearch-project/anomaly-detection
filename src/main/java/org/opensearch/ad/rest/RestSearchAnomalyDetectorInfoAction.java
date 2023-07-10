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

import static org.opensearch.ad.util.RestHandlerUtils.COUNT;
import static org.opensearch.ad.util.RestHandlerUtils.MATCH;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.SearchAnomalyDetectorInfoAction;
import org.opensearch.ad.transport.SearchAnomalyDetectorInfoRequest;
import org.opensearch.ad.transport.SearchAnomalyDetectorInfoResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.NamedRoute;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.rest.BaseExtensionRestHandler;
import org.opensearch.sdk.rest.ReplacedRouteHandler;

import com.google.common.collect.ImmutableList;

public class RestSearchAnomalyDetectorInfoAction extends BaseExtensionRestHandler {

    public static final String SEARCH_ANOMALY_DETECTOR_INFO_ACTION = "search_anomaly_detector_info";

    private static final Logger logger = LogManager.getLogger(RestSearchAnomalyDetectorInfoAction.class);
    private SDKRestClient sdkRestClient;
    private ExtensionsRunner extensionsRunner;

    public RestSearchAnomalyDetectorInfoAction(ExtensionsRunner extensionsRunner, SDKRestClient sdkRestClient) {
        this.extensionsRunner = extensionsRunner;
        this.sdkRestClient = sdkRestClient;
    }

    public String getName() {
        return SEARCH_ANOMALY_DETECTOR_INFO_ACTION;
    }

    private Function<RestRequest, RestResponse> handleRequest = (request) -> {
        try {
            return prepareRequest(request);
        } catch (Exception e) {
            return exceptionalRequest(request, e);
        }
    };

    protected ExtensionRestResponse prepareRequest(RestRequest request) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        String detectorName = request.param("name", null);
        String rawPath = request.rawPath();

        SearchAnomalyDetectorInfoRequest searchAnomalyDetectorInfoRequest = new SearchAnomalyDetectorInfoRequest(detectorName, rawPath);
        CompletableFuture<SearchAnomalyDetectorInfoResponse> futureResponse = new CompletableFuture<>();
        sdkRestClient
            .execute(
                SearchAnomalyDetectorInfoAction.INSTANCE,
                searchAnomalyDetectorInfoRequest,
                ActionListener
                    .wrap(
                        adSearchInfoResponse -> futureResponse.complete(adSearchInfoResponse),
                        ex -> futureResponse.completeExceptionally(ex)
                    )
            );

        SearchAnomalyDetectorInfoResponse searchAnomalyDetectorInfoResponse = futureResponse
            .orTimeout(
                AnomalyDetectorSettings.REQUEST_TIMEOUT.get(extensionsRunner.getEnvironmentSettings()).getMillis(),
                TimeUnit.MILLISECONDS
            )
            .join();
        ExtensionRestResponse extensionRestResponse = new ExtensionRestResponse(
            request,
            RestStatus.OK,
            searchAnomalyDetectorInfoResponse.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)
        );
        return extensionRestResponse;
    }

    @Override
    public List<NamedRoute> routes() {
        return ImmutableList.of();
    }

    @Override
    public List<ReplacedRouteHandler> replacedRouteHandlers() {
        return ImmutableList
            .of(
                // get the count of number of detectors
                new ReplacedRouteHandler(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/%s", AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, COUNT),
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/%s", AnomalyDetectorExtension.LEGACY_OPENDISTRO_AD_BASE_URI, COUNT),
                    handleRequest
                ),
                // get if a detector name exists with name
                new ReplacedRouteHandler(
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/%s", AnomalyDetectorExtension.AD_BASE_DETECTORS_URI, MATCH),
                    RestRequest.Method.GET,
                    String.format(Locale.ROOT, "%s/%s", AnomalyDetectorExtension.LEGACY_OPENDISTRO_AD_BASE_URI, MATCH),
                    handleRequest
                )
            );
    }
}
