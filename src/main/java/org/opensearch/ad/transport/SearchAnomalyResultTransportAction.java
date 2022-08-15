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
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import com.google.common.annotations.VisibleForTesting;

public class SearchAnomalyResultTransportAction extends HandledTransportAction<SearchRequest, SearchResponse> {
    public static final String RESULT_INDEX_AGG_NAME = "result_index";

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

    @VisibleForTesting
    boolean validateIndexAndReturnOnlyQueryCustomResult(String[] indices) {
        if (indices == null || indices.length == 0) {
            throw new IllegalArgumentException("No indices set in search request");
        }
        // Set query indices as default result indices, will check custom result indices permission and add
        // custom indices which user has search permission later.

        boolean onlyQueryCustomResultIndex = true;
        for (String indexName : indices) {
            // If only query custom result index, don't need to set ALL_AD_RESULTS_INDEX_PATTERN in search request
            if (ALL_AD_RESULTS_INDEX_PATTERN.equals(indexName)) {
                onlyQueryCustomResultIndex = false;
            }
        }
        return onlyQueryCustomResultIndex;
    }

    @VisibleForTesting
    void calculateCustomResultIndices(Set<String> customResultIndices, String[] indices) {
        String[] concreteIndices = indexNameExpressionResolver
            .concreteIndexNames(clusterService.state(), IndicesOptions.lenientExpandOpen(), indices);
        // If concreteIndices is null or empty, don't throw exception. Detector list page will search both
        // default and custom result indices to get anomaly of last 24 hours. If throw exception, detector
        // list page will throw error and won't show any detector.
        // If a cluster has no custom result indices, and some new non-custom-result-detector that hasn't
        // finished one interval (where no default result index exists), then no result indices found. We
        // will still search ".opendistro-anomaly-results*" (even these default indices don't exist) to
        // return an empty SearchResponse. This search looks unnecessary, but this can make sure the
        // detector list page show all detectors correctly. The other solution is to catch errors from
        // frontend when search anomaly results to make sure frontend won't crash. Check this Github issue:
        // https://github.com/opensearch-project/anomaly-detection-dashboards-plugin/issues/154
        if (concreteIndices != null) {
            for (String index : concreteIndices) {
                if (index.startsWith(CUSTOM_RESULT_INDEX_PREFIX)) {
                    customResultIndices.add(index);
                }
            }
        }
    }

    @VisibleForTesting
    SearchRequest createSingleSearchRequest() {
        // Search both custom AD result index and default result index
        SearchSourceBuilder searchResultIndexBuilder = new SearchSourceBuilder();
        AggregationBuilder aggregation = new TermsAggregationBuilder(RESULT_INDEX_AGG_NAME)
            .field(AnomalyDetector.RESULT_INDEX_FIELD)
            .size(MAX_DETECTOR_UPPER_LIMIT);
        searchResultIndexBuilder.aggregation(aggregation).size(0);
        return new SearchRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).source(searchResultIndexBuilder);
    }

    @VisibleForTesting
    void processSingleSearchResponse(
        SearchResponse allResultIndicesResponse,
        SearchRequest request,
        ActionListener<SearchResponse> listener,
        Set<String> customResultIndices,
        List<String> targetIndices
    ) {
        Aggregations aggregations = allResultIndicesResponse.getAggregations();
        StringTerms resultIndicesAgg = aggregations.get(RESULT_INDEX_AGG_NAME);
        List<StringTerms.Bucket> buckets = resultIndicesAgg.getBuckets();
        Set<String> resultIndicesOfDetector = new HashSet<>();
        if (buckets == null) {
            searchHandler.search(request, listener);
            return;
        }
        buckets.stream().forEach(b -> resultIndicesOfDetector.add(b.getKeyAsString()));
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
    }

    @VisibleForTesting
    MultiSearchRequest createMultiSearchRequest(List<String> targetIndices) {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (String index : targetIndices) {
            multiSearchRequest.add(new SearchRequest(index).source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(0)));
        }
        return multiSearchRequest;
    }

    @VisibleForTesting
    void multiSearch(
        List<String> targetIndices,
        SearchRequest request,
        ActionListener<SearchResponse> listener,
        boolean finalOnlyQueryCustomResultIndex
    ) {
        if (targetIndices.size() == 0) {
            // no need to make multi search
            return;
        }
        MultiSearchRequest multiSearchRequest = createMultiSearchRequest(targetIndices);
        List<String> readableIndices = new ArrayList<>();
        if (!finalOnlyQueryCustomResultIndex) {
            readableIndices.add(ALL_AD_RESULTS_INDEX_PATTERN);
        }

        // Send multiple search to check which index a user has permission to read. If search all indices directly,
        // search request will throw exception if user has no permission to search any index.
        client
            .multiSearch(
                multiSearchRequest,
                ActionListener
                    .wrap(
                        multiSearchResponse -> {
                            processMultiSearchResponse(multiSearchResponse, targetIndices, readableIndices, request, listener);
                        },
                        multiSearchException -> {
                            logger.error("Failed to search custom AD result indices", multiSearchException);
                            listener.onFailure(multiSearchException);
                        }
                    )
            );
    }

    @VisibleForTesting
    void processMultiSearchResponse(
        MultiSearchResponse multiSearchResponse,
        List<String> targetIndices,
        List<String> readableIndices,
        SearchRequest request,
        ActionListener<SearchResponse> listener
    ) {
        MultiSearchResponse.Item[] responses = multiSearchResponse.getResponses();
        for (int i = 0; i < responses.length; i++) {
            MultiSearchResponse.Item item = responses[i];
            String indexName = targetIndices.get(i);
            if (item.getFailure() == null) {
                readableIndices.add(indexName);
            }
        }
        if (readableIndices.size() == 0) {
            listener.onFailure(new IllegalArgumentException("No readable custom result indices found"));
            return;
        }
        request.indices(readableIndices.toArray(new String[0]));
        searchHandler.search(request, listener);
    }

    @VisibleForTesting
    void searchADResultIndex(
        SearchRequest request,
        ActionListener<SearchResponse> listener,
        boolean onlyQueryCustomResultIndex,
        Set<String> customResultIndices
    ) {
        SearchRequest searchResultIndex = createSingleSearchRequest();
        try {
            // Search result indices of all detectors. User may create index with same prefix of custom result index
            // which not used for AD, so we should avoid searching extra indices which not used by anomaly detectors.
            // Variable used in lambda expression should be final or effectively final, so copy to a final boolean and
            // use the final boolean in lambda below.
            boolean finalOnlyQueryCustomResultIndex = onlyQueryCustomResultIndex;
            client.search(searchResultIndex, ActionListener.wrap(allResultIndicesResponse -> {
                List<String> targetIndices = new ArrayList<>();
                processSingleSearchResponse(allResultIndicesResponse, request, listener, customResultIndices, targetIndices);
                multiSearch(targetIndices, request, listener, finalOnlyQueryCustomResultIndex);
            }, e -> {
                logger.error("Failed to search result indices for all detectors", e);
                listener.onFailure(e);
            }));
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
        boolean onlyQueryCustomResultIndex;
        Set<String> customResultIndices = new HashSet<>();

        try {
            onlyQueryCustomResultIndex = validateIndexAndReturnOnlyQueryCustomResult(request.indices());
            calculateCustomResultIndices(customResultIndices, request.indices());
            // If user need to query custom result index only, and that custom result index deleted. Then
            // we should not search anymore. Just throw exception here.
            if (onlyQueryCustomResultIndex && customResultIndices.size() == 0) {
                throw new IllegalArgumentException("No custom result indices found");
            }
        } catch (IllegalArgumentException exception) {
            listener.onFailure(exception);
            return;
        }

        if (customResultIndices.size() == 0) {
            // onlyQueryCustomResultIndex is false in this branch
            // Search only default result index
            request.indices(ALL_AD_RESULTS_INDEX_PATTERN);
            searchHandler.search(request, listener);
            return;
        }

        searchADResultIndex(request, listener, onlyQueryCustomResultIndex, customResultIndices);
    }

}
