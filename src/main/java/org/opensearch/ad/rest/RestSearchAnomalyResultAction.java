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

import static org.opensearch.ad.constant.CommonErrorMessages.CAN_NOT_FIND_INDEX;
import static org.opensearch.ad.indices.AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_DETECTOR_UPPER_LIMIT;
import static org.opensearch.ad.util.RestHandlerUtils.RESULT_INDEX;
import static org.opensearch.ad.util.RestHandlerUtils.getSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.transport.SearchAnomalyResultAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to search anomaly results.
 */
public class RestSearchAnomalyResultAction extends AbstractSearchAction<AnomalyResult> {
    private final Logger logger = LogManager.getLogger(RestSearchAnomalyResultAction.class);
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
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.parseXContent(request.contentOrSourceParamParser());
        searchSourceBuilder.fetchSource(getSourceContext(request));
        searchSourceBuilder.seqNoAndPrimaryTerm(true).version(true);
        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder).indices(this.index);

        String resultIndex = SEARCH_ANOMALY_RESULT_ACTION.equals(getName()) ? Strings.trimToNull(request.param(RESULT_INDEX)) : null;
        if (resultIndex == null) {
            return channel -> client.execute(actionType, searchRequest, search(channel));
        }
        return channel -> {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                // Get all concrete indices
                client.admin().indices().prepareGetIndex().setIndices(resultIndex).execute(ActionListener.wrap(getIndexResponse -> {
                    String[] indices = getIndexResponse.getIndices();
                    if (indices == null || indices.length == 0) {
                        onFailure(channel, new ResourceNotFoundException(CAN_NOT_FIND_INDEX));
                        return;
                    }
                    String resultIndexAggName = "result_index";
                    SearchSourceBuilder searchResultIndexBuilder = new SearchSourceBuilder();
                    AggregationBuilder aggregation = new TermsAggregationBuilder(resultIndexAggName)
                        .field(AnomalyDetector.RESULT_INDEX_FIELD)
                        .size(MAX_DETECTOR_UPPER_LIMIT);
                    searchResultIndexBuilder.aggregation(aggregation).size(0);
                    SearchRequest searchResultIndex = new SearchRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX)
                        .source(searchResultIndexBuilder);

                    // Search result indices of all detectors
                    client.search(searchResultIndex, ActionListener.wrap(allResultIndicesResponse -> {
                        context.restore();
                        Aggregations aggregations = allResultIndicesResponse.getAggregations();
                        StringTerms resultIndicesAgg = aggregations.get(resultIndexAggName);
                        List<StringTerms.Bucket> buckets = resultIndicesAgg.getBuckets();
                        Set<String> resultIndicesOfDetector = new HashSet<>();
                        if (buckets != null) {
                            buckets.stream().forEach(b -> resultIndicesOfDetector.add(b.getKeyAsString()));
                        }
                        List<String> targetIndices = new ArrayList<>();
                        for (String index : indices) {
                            if (resultIndicesOfDetector.contains(index)) {
                                targetIndices.add(index);
                            }
                        }
                        if (targetIndices.size() == 0) {
                            // No custom result indices used by detectors, just search default result index
                            client.execute(actionType, searchRequest, search(channel));
                            return;
                        }

                        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
                        for (String index : targetIndices) {
                            multiSearchRequest
                                .add(new SearchRequest(index).source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(0)));
                        }
                        List<String> readableIndices = new ArrayList<>();
                        client.multiSearch(multiSearchRequest, ActionListener.wrap(multiSearchResponse -> {
                            MultiSearchResponse.Item[] responses = multiSearchResponse.getResponses();
                            for (int i = 0; i < responses.length; i++) {
                                MultiSearchResponse.Item item = responses[i];
                                String indexName = targetIndices.get(i);
                                if (item.getFailure() == null) {
                                    readableIndices.add(indexName);
                                } else {
                                    logger.debug("Failed to search index {}, {}", indexName, item.getFailureMessage());
                                }
                            }
                            logger
                                .debug(
                                    "Will search anomaly result from these indices : {}",
                                    Arrays.toString(readableIndices.toArray(new String[0]))
                                );

                            // search anomaly results for all readable result indices and default result index
                            readableIndices.add(this.index);
                            executeWithAdmin(client, () -> {
                                searchRequest.indices(readableIndices.toArray(new String[0]));
                                client.search(searchRequest, search(channel));
                            }, channel);
                        }, multiSearchException -> {
                            logger.error("Failed to search multiple indices", multiSearchException);
                            onFailure(channel, multiSearchException);
                        }));
                    }, ex -> {
                        logger.error("Failed to search result indices for all detectors", ex);
                        onFailure(channel, ex);
                    }));
                }, getIndexException -> {
                    logger.error("Failed to get concrete indices for " + resultIndex, getIndexException);
                    onFailure(channel, getIndexException);
                }));
            } catch (Exception e) {
                onFailure(channel, e);
            }
        };
    }

}
