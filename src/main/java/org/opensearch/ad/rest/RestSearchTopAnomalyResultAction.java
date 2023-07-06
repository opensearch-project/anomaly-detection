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

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.opensearch.action.ActionListener;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.SearchTopAnomalyResultAction;
import org.opensearch.ad.transport.SearchTopAnomalyResultRequest;
import org.opensearch.ad.transport.SearchTopAnomalyResultResponse;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.NamedRoute;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.rest.BaseExtensionRestHandler;

import com.google.common.collect.ImmutableList;

/**
 * The REST handler to search top entity anomaly results for HC detectors.
 */
public class RestSearchTopAnomalyResultAction extends BaseExtensionRestHandler {

    private SDKRestClient sdkRestClient;
    private ExtensionsRunner extensionsRunner;
    private static final String URL_PATH = String
        .format(
            Locale.ROOT,
            "%s/{%s}/%s/%s",
            AnomalyDetectorExtension.AD_BASE_DETECTORS_URI,
            RestHandlerUtils.DETECTOR_ID,
            RestHandlerUtils.RESULTS,
            RestHandlerUtils.TOP_ANOMALIES
        );
    private final String SEARCH_TOP_ANOMALY_DETECTOR_ACTION = "search_top_anomaly_result";

    public RestSearchTopAnomalyResultAction(ExtensionsRunner extensionsRunner, SDKRestClient sdkRestClient) {
        this.sdkRestClient = sdkRestClient;
        this.extensionsRunner = extensionsRunner;
    }

    public String getName() {
        return SEARCH_TOP_ANOMALY_DETECTOR_ACTION;
    }

    private Function<RestRequest, RestResponse> handleRequest = (request) -> {
        try {
            return prepareRequest(request);
        } catch (Exception e) {
            // TODO: handle the AD-specific exceptions separately
            return exceptionalRequest(request, e);
        }
    };

    protected ExtensionRestResponse prepareRequest(RestRequest request) throws IOException {

        // Throw error if disabled
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        // Get the typed request
        SearchTopAnomalyResultRequest searchTopAnomalyResultRequest = getSearchTopAnomalyResultRequest(request);

        CompletableFuture<SearchTopAnomalyResultResponse> futureResponse = new CompletableFuture<>();

        sdkRestClient
            .execute(
                SearchTopAnomalyResultAction.INSTANCE,
                searchTopAnomalyResultRequest,
                ActionListener
                    .wrap(
                        adSearchTopAnomaliesResponse -> futureResponse.complete(adSearchTopAnomaliesResponse),
                        ex -> futureResponse.completeExceptionally(ex)
                    )
            );

        SearchTopAnomalyResultResponse searchTopAnomalyResultResponse = futureResponse
            .orTimeout(
                AnomalyDetectorSettings.REQUEST_TIMEOUT.get(extensionsRunner.getEnvironmentSettings()).getMillis(),
                TimeUnit.MILLISECONDS
            )
            .join();

        ExtensionRestResponse extensionRestResponse = new ExtensionRestResponse(
            request,
            RestStatus.OK,
            searchTopAnomalyResultResponse.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)
        );

        return extensionRestResponse;

    }

    private SearchTopAnomalyResultRequest getSearchTopAnomalyResultRequest(RestRequest request) throws IOException {
        String detectorId;
        if (request.hasParam(RestHandlerUtils.DETECTOR_ID)) {
            detectorId = request.param(RestHandlerUtils.DETECTOR_ID);
        } else {
            throw new IllegalStateException(CommonErrorMessages.AD_ID_MISSING_MSG);
        }
        boolean historical = request.paramAsBoolean("historical", false);
        XContentParser parser = request.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        return SearchTopAnomalyResultRequest.parse(parser, detectorId, historical);
    }

    @Override
    public List<NamedRoute> routes() {
        return ImmutableList
            .of(
                new NamedRoute.Builder()
                    .method(POST)
                    .path(URL_PATH)
                    .uniqueName(routePrefix("search/post/topresults"))
                    .handler(handleRequest)
                    .build(),
                new NamedRoute.Builder()
                    .method(GET)
                    .path(URL_PATH)
                    .uniqueName(routePrefix("search/read/topresults"))
                    .handler(handleRequest)
                    .build()
            );
    }
}
