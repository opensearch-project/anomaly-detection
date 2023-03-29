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

import static org.opensearch.ad.util.RestHandlerUtils.getSourceContext;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.AnomalyDetectorFunction;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.extensions.rest.ExtensionRestRequest;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.sdk.BaseExtensionRestHandler;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.RouteHandler;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Abstract class to handle search request.
 */
public abstract class AbstractSearchAction<T extends ToXContentObject> extends BaseExtensionRestHandler {

    protected final String index;
    protected final Class<T> clazz;
    protected final List<String> urlPaths;
    protected final List<Pair<String, String>> deprecatedPaths;
    protected final ActionType<SearchResponse> actionType;
    private ExtensionsRunner extensionsRunner;
    private SDKRestClient client;

    private final Logger logger = LogManager.getLogger(AbstractSearchAction.class);

    public AbstractSearchAction(
        List<String> urlPaths,
        List<Pair<String, String>> deprecatedPaths,
        String index,
        Class<T> clazz,
        ActionType<SearchResponse> actionType,
        SDKRestClient client,
        ExtensionsRunner extensionsRunner
    ) {
        this.index = index;
        this.clazz = clazz;
        this.urlPaths = urlPaths;
        this.deprecatedPaths = deprecatedPaths;
        this.actionType = actionType;
        this.client = client;
        this.extensionsRunner = extensionsRunner;
    }

    private Function<ExtensionRestRequest, ExtensionRestResponse> handleRequest = (request) -> {
        try {
            return prepareRequest(request);
        } catch (Exception e) {
            // TODO: handle the AD-specific exceptions separately
            return exceptionalRequest(request, e);
        }
    };

    protected ExtensionRestResponse prepareRequest(ExtensionRestRequest request) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.parseXContent(request.contentOrSourceParamParser());
        searchSourceBuilder.fetchSource(getSourceContext(request));
        searchSourceBuilder.seqNoAndPrimaryTerm(true).version(true);
        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder).indices(this.index);
        CompletableFuture<SearchResponse> futureResponse = new CompletableFuture<>();
        client.execute(actionType, searchRequest,  ActionListener
                .wrap(adSearchResponse -> futureResponse.complete(adSearchResponse), ex -> futureResponse.completeExceptionally(ex)));

        SearchResponse searchResponse = futureResponse.orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(extensionsRunner.getEnvironmentSettings()).getMillis(), TimeUnit.MILLISECONDS).join();
        return search(request, searchResponse);
    }

    protected void executeWithAdmin(NodeClient client, AnomalyDetectorFunction function, RestChannel channel) {
        try {
            function.execute();
        } catch (Exception e) {
            logger.error("Failed to execute with admin", e);
            onFailure(channel, e);
        }
    }

    protected void onFailure(RestChannel channel, Exception e) {
        try {
            channel.sendResponse(new BytesRestResponse(channel, e));
        } catch (Exception exception) {
            logger.error("Failed to send back failure response for search AD result", exception);
        }
    }

    protected ExtensionRestResponse search(ExtensionRestRequest request, SearchResponse response) throws IOException {
//        return new RestResponseListener<SearchResponse>(channel) {
//            @Override
//            public RestResponse buildResponse(SearchResponse response) throws Exception {
                if (response.isTimedOut()) {
                    return new ExtensionRestResponse(request, RestStatus.REQUEST_TIMEOUT, response.toString());
                }

                if (clazz == AnomalyDetector.class) {
                    for (SearchHit hit : response.getHits()) {
                        XContentParser parser = XContentType.JSON
                            .xContent()
                            .createParser(
                                extensionsRunner.getNamedXContentRegistry().getRegistry(),
                                LoggingDeprecationHandler.INSTANCE,
                                hit.getSourceAsString()
                            );
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

                        // write back id and version to anomaly detector object
                        ToXContentObject xContentObject = AnomalyDetector.parse(parser, hit.getId(), hit.getVersion());
                        XContentBuilder builder = xContentObject.toXContent(jsonBuilder(), EMPTY_PARAMS);
                        hit.sourceRef(BytesReference.bytes(builder));
                    }
                }

                return new ExtensionRestResponse(request, RestStatus.OK, response.toXContent(JsonXContent.contentBuilder(), EMPTY_PARAMS));
    }


    public List<RouteHandler> routeHandlers() {
        return ImmutableList
                .of(
                        new RouteHandler(RestRequest.Method.GET, AnomalyDetectorExtension.AD_BASE_DETECTORS_URI + "_search", handleRequest)
                );
    }

    /*@Override
    public List<Route> routes() {
        List<Route> routes = new ArrayList<>();
        for (String path : urlPaths) {
            routes.add(new Route(RestRequest.Method.POST, path));
            routes.add(new Route(RestRequest.Method.GET, path));
        }
        return routes;
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        List<ReplacedRoute> replacedRoutes = new ArrayList<>();
        for (Pair<String, String> deprecatedPath : deprecatedPaths) {
            replacedRoutes
                .add(
                    new ReplacedRoute(RestRequest.Method.POST, deprecatedPath.getKey(), RestRequest.Method.POST, deprecatedPath.getValue())
                );
            replacedRoutes
                .add(new ReplacedRoute(RestRequest.Method.GET, deprecatedPath.getKey(), RestRequest.Method.GET, deprecatedPath.getValue()));

        }
        return replacedRoutes;
    }*/
}
