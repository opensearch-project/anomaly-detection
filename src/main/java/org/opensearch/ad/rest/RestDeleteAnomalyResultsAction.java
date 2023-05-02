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

import static org.opensearch.ad.indices.AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.DeleteAnomalyResultsAction;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.rest.BaseExtensionRestHandler;
import org.opensearch.search.builder.SearchSourceBuilder;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to delete anomaly result with specific query.
 * Currently AD dashboard plugin doesn't call this API. User can use this API to delete
 * anomaly results to free up disk space.
 *
 * User needs to delete anomaly result from custom result index by themselves as they
 * can directly access these custom result index.
 * Same strategy for custom result index rollover. Suggest user using ISM plugin to
 * manage custom result index.
 *
 * TODO: build better user experience to reduce user's effort to maintain custom result index.
 */
public class RestDeleteAnomalyResultsAction extends BaseExtensionRestHandler {

    private static final String DELETE_AD_RESULTS_ACTION = "delete_anomaly_results";
    private static final Logger logger = LogManager.getLogger(RestDeleteAnomalyResultsAction.class);
    private ExtensionsRunner extensionsRunner;
    private SDKRestClient sdkRestClient;

    public RestDeleteAnomalyResultsAction(ExtensionsRunner extensionsRunner, SDKRestClient client) {
        this.extensionsRunner = extensionsRunner;
        this.sdkRestClient = client;
    }

    public String getName() {
        return DELETE_AD_RESULTS_ACTION;
    }

    private Function<RestRequest, ExtensionRestResponse> handleRequest = (request) -> {
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
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.parseXContent(request.contentOrSourceParamParser());
        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(ALL_AD_RESULTS_INDEX_PATTERN)
            .setQuery(searchSourceBuilder.query())
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN);
        CompletableFuture<BulkByScrollResponse> futureResponse = new CompletableFuture<>();
        sdkRestClient
            .execute(
                DeleteAnomalyResultsAction.INSTANCE,
                deleteRequest,
                ActionListener
                    .wrap(deleteResponse -> futureResponse.complete(deleteResponse), ex -> futureResponse.completeExceptionally(ex))
            );
        BulkByScrollResponse bulkByScrollResponse = futureResponse
            .orTimeout(
                AnomalyDetectorSettings.REQUEST_TIMEOUT.get(extensionsRunner.getEnvironmentSettings()).getMillis(),
                TimeUnit.MILLISECONDS
            )
            .join();

        XContentBuilder contentBuilder = bulkByScrollResponse
            .toXContent(JsonXContent.contentBuilder().startObject(), ToXContent.EMPTY_PARAMS);
        contentBuilder.endObject();

        ExtensionRestResponse extensionRestResponse = new ExtensionRestResponse(request, RestStatus.OK, contentBuilder);
        return extensionRestResponse;
    }

    @Override
    public List<RouteHandler> routeHandlers() {
        return ImmutableList
            .of(new RouteHandler(RestRequest.Method.DELETE, AnomalyDetectorExtension.AD_BASE_DETECTORS_URI + "/results", handleRequest));
    }
}
