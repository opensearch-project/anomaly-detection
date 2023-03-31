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
import static org.opensearch.ad.util.RestHandlerUtils.RESULT_INDEX;
import static org.opensearch.ad.util.RestHandlerUtils.getSourceContext;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.AnomalyDetectorExtension;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.SearchAnomalyResultAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.extensions.rest.ExtensionRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.search.builder.SearchSourceBuilder;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to search anomaly results.
 */
public class RestSearchAnomalyResultAction extends AbstractSearchAction<AnomalyResult> {
    private static final String LEGACY_URL_PATH = AnomalyDetectorPlugin.LEGACY_OPENDISTRO_AD_BASE_URI + "/results/_search";
    private static final String URL_PATH = AnomalyDetectorExtension.AD_BASE_DETECTORS_URI + "/results/_search";
    public static final String SEARCH_ANOMALY_RESULT_ACTION = "search_anomaly_result";
    private SDKRestClient client;
    private ExtensionsRunner extensionsRunner;

    public RestSearchAnomalyResultAction(ExtensionsRunner extensionsRunner, SDKRestClient client) {
        super(
            ImmutableList.of(String.format(Locale.ROOT, "%s/{%s}", URL_PATH, RESULT_INDEX)),
            ImmutableList.of(Pair.of(URL_PATH, LEGACY_URL_PATH)),
            ALL_AD_RESULTS_INDEX_PATTERN,
            AnomalyResult.class,
            SearchAnomalyResultAction.INSTANCE,
            client,
            extensionsRunner
        );
    }

    public String getName() {
        return SEARCH_ANOMALY_RESULT_ACTION;
    }

    protected ExtensionRestResponse prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (!EnabledSetting.isADPluginEnabled()) {
            throw new IllegalStateException(CommonErrorMessages.DISABLED_ERR_MSG);
        }

        // resultIndex could be concrete index or index pattern
        String resultIndex = Strings.trimToNull(request.param(RESULT_INDEX));
        boolean onlyQueryCustomResultIndex = request.paramAsBoolean("only_query_custom_result_index", false);
        if (resultIndex == null && onlyQueryCustomResultIndex) {
            throw new IllegalStateException("No custom result index set.");
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.parseXContent(request.contentOrSourceParamParser());
        searchSourceBuilder.fetchSource(getSourceContext(request));
        searchSourceBuilder.seqNoAndPrimaryTerm(true).version(true);
        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder).indices(this.index);

        if (resultIndex != null) {
            if (onlyQueryCustomResultIndex) {
                searchRequest.indices(resultIndex);
            } else {
                searchRequest.indices(this.index, resultIndex);
            }
        }
        CompletableFuture<SearchResponse> futureResponse = new CompletableFuture<>();
        client
            .execute(
                actionType,
                searchRequest,
                ActionListener
                    .wrap(adSearchResponse -> futureResponse.complete(adSearchResponse), ex -> futureResponse.completeExceptionally(ex))
            );
        SearchResponse searchResponse = futureResponse
            .orTimeout(
                AnomalyDetectorSettings.REQUEST_TIMEOUT.get(extensionsRunner.getEnvironmentSettings()).getMillis(),
                TimeUnit.MILLISECONDS
            )
            .join();
        return search(request, searchResponse);
    }

}
