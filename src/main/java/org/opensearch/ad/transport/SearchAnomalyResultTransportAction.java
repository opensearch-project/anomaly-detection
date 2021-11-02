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

package org.opensearch.ad.transport;

import static org.opensearch.ad.constant.CommonName.CUSTOM_RESULT_INDEX_PREFIX;
import static org.opensearch.ad.indices.AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_DETECTOR_UPPER_LIMIT;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.transport.handler.ADSearchHandler;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class SearchAnomalyResultTransportAction extends HandledTransportAction<SearchRequest, SearchResponse> {

    private final Logger logger = LogManager.getLogger(SearchAnomalyResultTransportAction.class);
    private ADSearchHandler searchHandler;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Client client;

    @Inject
    public SearchAnomalyResultTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ADSearchHandler searchHandler,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(SearchAnomalyResultAction.NAME, transportService, actionFilters, SearchRequest::new);
        this.searchHandler = searchHandler;
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
        String[] indices = request.indices();
        // Set query indices as default result indices, will check custom result indices permission and add
        // custom indices which user has search permission later.
        request.indices(ALL_AD_RESULTS_INDEX_PATTERN);

        Set<String> customResultIndices = new HashSet<>();
        if (indices != null && indices.length > 0) {
            String[] concreteIndices = indexNameExpressionResolver
                .concreteIndexNames(clusterService.state(), IndicesOptions.lenientExpandOpen(), indices);
            if (concreteIndices == null || concreteIndices.length == 0) {
                // No custom result indices found, just search default result index
                searchHandler.search(request, listener);
                return;
            }
            for (String index : concreteIndices) {
                if (index.startsWith(CUSTOM_RESULT_INDEX_PREFIX)) {
                    customResultIndices.add(index);
                }
            }
        }

        if (customResultIndices.size() > 0) {
            // Search both custom AD result index and default result index
            String resultIndexAggName = "result_index";
            SearchSourceBuilder searchResultIndexBuilder = new SearchSourceBuilder();
            AggregationBuilder aggregation = new TermsAggregationBuilder(resultIndexAggName)
                .field(AnomalyDetector.RESULT_INDEX_FIELD)
                .size(MAX_DETECTOR_UPPER_LIMIT);
            searchResultIndexBuilder.aggregation(aggregation).size(0);
            SearchRequest searchResultIndex = new SearchRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).source(searchResultIndexBuilder);
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                // Search result indices of all detectors. User may create index with same prefix of custom result index
                // which not used for AD, so we should avoid searching extra indices which not used by anomaly detectors.
                client.search(searchResultIndex, ActionListener.wrap(allResultIndicesResponse -> {
                    Aggregations aggregations = allResultIndicesResponse.getAggregations();
                    StringTerms resultIndicesAgg = aggregations.get(resultIndexAggName);
                    List<StringTerms.Bucket> buckets = resultIndicesAgg.getBuckets();
                    Set<String> resultIndicesOfDetector = new HashSet<>();
                    if (buckets == null) {
                        searchHandler.search(request, listener);
                        return;
                    }
                    buckets.stream().forEach(b -> resultIndicesOfDetector.add(b.getKeyAsString()));
                    List<String> targetIndices = new ArrayList<>();
                    for (String index : customResultIndices) {
                        if (resultIndicesOfDetector.contains(index)) {
                            targetIndices.add(index);
                        }
                    }
                    if (targetIndices.size() == 0) {
                        // No custom result indices used by detectors, just search default result index
                        searchHandler.search(request, listener);
                        return;
                    }
                    MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
                    for (String index : targetIndices) {
                        multiSearchRequest
                            .add(new SearchRequest(index).source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(0)));
                    }
                    List<String> readableIndices = new ArrayList<>();
                    readableIndices.add(ALL_AD_RESULTS_INDEX_PATTERN);

                    context.restore();
                    // Send multiple search to check which index a user has permission to read. If search all indices directly,
                    // search request will throw exception if user has no permission to search any index.
                    client.multiSearch(multiSearchRequest, ActionListener.wrap(multiSearchResponse -> {
                        MultiSearchResponse.Item[] responses = multiSearchResponse.getResponses();
                        for (int i = 0; i < responses.length; i++) {
                            MultiSearchResponse.Item item = responses[i];
                            String indexName = targetIndices.get(i);
                            if (item.getFailure() == null) {
                                readableIndices.add(indexName);
                            }
                        }
                        request.indices(readableIndices.toArray(new String[0]));
                        searchHandler.search(request, listener);
                    }, multiSearchException -> {
                        logger.error("Failed to search custom AD result indices", multiSearchException);
                        listener.onFailure(multiSearchException);
                    }));
                }, e -> {
                    logger.error("Failed to search result indices for all detectors", e);
                    listener.onFailure(e);
                }));
            } catch (Exception e) {
                logger.error(e);
                listener.onFailure(e);
            }
        } else {
            // Search only default result index
            searchHandler.search(request, listener);
        }
    }

}
