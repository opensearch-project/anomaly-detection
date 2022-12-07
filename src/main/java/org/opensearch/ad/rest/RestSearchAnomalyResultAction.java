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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.SearchAnomalyResultAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to search anomaly results.
 */
public class RestSearchAnomalyResultAction extends AbstractSearchAction<AnomalyResult> {
    private static final String LEGACY_URL_PATH = AnomalyDetectorPlugin.LEGACY_OPENDISTRO_AD_BASE_URI + "/results/_search";
    private static final String URL_PATH = AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI + "/results/_search";
    public static final String SEARCH_ANOMALY_RESULT_ACTION = "search_anomaly_result";

    public RestSearchAnomalyResultAction() {
        super(
            ImmutableList.of(String.format(Locale.ROOT, "%s/{%s}", URL_PATH, RESULT_INDEX)),
            ImmutableList.of(Pair.of(URL_PATH, LEGACY_URL_PATH)),
            ALL_AD_RESULTS_INDEX_PATTERN,
            AnomalyResult.class,
            SearchAnomalyResultAction.INSTANCE
        );
    }

    @Override
    public String getName() {
        return SEARCH_ANOMALY_RESULT_ACTION;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
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
        searchSourceBuilder.fetchSource(getSourceContext(request, searchSourceBuilder));
        searchSourceBuilder.seqNoAndPrimaryTerm(true).version(true);
        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder).indices(this.index);

        if (resultIndex != null) {
            if (onlyQueryCustomResultIndex) {
                searchRequest.indices(resultIndex);
            } else {
                searchRequest.indices(this.index, resultIndex);
            }
        }
        return channel -> client.execute(actionType, searchRequest, search(channel));
    }

}
