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

import static org.opensearch.ad.indices.ADIndexManagement.ALL_AD_RESULTS_INDEX_PATTERN;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.TOP_ANOMALY_RESULT_TIMEOUT_IN_MILLIS;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.AnomalyResultBucket;
import org.opensearch.ad.transport.handler.ADSearchHandler;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.pipeline.BucketSortPipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.transport.GetConfigRequest;
import org.opensearch.timeseries.util.QueryUtil;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Transport action to fetch top anomaly results for some HC detector. Generates a
 * query based on user input to fetch aggregated entity results.
 */

// Example of a generated query aggregating over the "Carrier" category field, and sorting on max anomaly grade, using
// a historical task ID:
//
// {
// "query": {
// "bool": {
// "filter": [
// {
// "range": {
// "data_end_time": {
// "from": "2021-09-10T07:00:00.000Z",
// "to": "2021-09-30T07:00:00.000Z",
// "include_lower": true,
// "include_upper": true,
// "boost": 1.0
// }
// }
// },
// {
// "range": {
// "anomaly_grade": {
// "from": 0,
// "include_lower": false,
// "include_upper": true,
// "boost": 1.0
// }
// }
// },
// {
// "term": {
// "task_id": {
// "value": "2AwACXwBM-RcgLq7Za87",
// "boost": 1.0
// }
// }
// }
// ],
// "adjust_pure_negative": true,
// "boost": 1.0
// }
// },
// "aggregations": {
// "multi_buckets": {
// "composite": {
// "size": 100,
// "sources": [
// {
// "Carrier": {
// "terms": {
// "script": {
// "source": """
// String value = null;
// if (params == null || params._source == null || params._source.entity == null) {
// return "";
// }
// for (item in params._source.entity) {
// if (item["name"] == params["categoryField"]) {
// value = item['value'];
// break;
// }
// }
// return value;
// """,
// "lang": "painless",
// "params": {
// "categoryField": "Carrier"
// }
// },
// "missing_bucket": false,
// "order": "asc"
// }
// }
// }
// ]
// },
// "aggregations": {
// "max_anomaly_grade": {
// "max": {
// "field": "anomaly_grade"
// }
// },
// "bucket_sort": {
// "bucket_sort": {
// "sort": [
// {
// "max_anomaly_grade": {
// "order": "desc"
// }
// }
// ],
// "from": 0,
// "gap_policy": "SKIP"
// }
// }
// }
// }
// }
// }

public class SearchTopAnomalyResultTransportAction extends
    HandledTransportAction<SearchTopAnomalyResultRequest, SearchTopAnomalyResultResponse> {
    private ADSearchHandler searchHandler;
    // Number of buckets to return per page
    private static final int PAGE_SIZE = 1000;
    private static final OrderType DEFAULT_ORDER_TYPE = OrderType.SEVERITY;
    private static final int DEFAULT_SIZE = 10;
    private static final int MAX_SIZE = 1000;
    private static final String defaultIndex = ALL_AD_RESULTS_INDEX_PATTERN;
    private static final String COUNT_FIELD = "_count";
    private static final String BUCKET_SORT_FIELD = "bucket_sort";
    public static final String MULTI_BUCKETS_FIELD = "multi_buckets";
    private static final Logger logger = LogManager.getLogger(SearchTopAnomalyResultTransportAction.class);
    private final Client client;
    private Clock clock;

    public enum OrderType {
        SEVERITY("severity"),
        OCCURRENCE("occurrence");

        private String name;

        OrderType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    @Inject
    public SearchTopAnomalyResultTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ADSearchHandler searchHandler,
        Client client
    ) {
        super(SearchTopAnomalyResultAction.NAME, transportService, actionFilters, SearchTopAnomalyResultRequest::new);
        this.searchHandler = searchHandler;
        this.client = client;
        this.clock = Clock.systemUTC();
    }

    @Override
    protected void doExecute(Task task, SearchTopAnomalyResultRequest request, ActionListener<SearchTopAnomalyResultResponse> listener) {

        GetConfigRequest getAdRequest = new GetConfigRequest(
            request.getId(),
            // The default version value used in org.opensearch.rest.action.RestActions.parseVersion()
            -3L,
            false,
            true,
            "",
            "",
            false,
            null
        );
        client.execute(GetAnomalyDetectorAction.INSTANCE, getAdRequest, ActionListener.wrap(getAdResponse -> {
            // Make sure detector exists
            if (getAdResponse.getDetector() == null) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "No anomaly detector found with ID %s", request.getId()));
            }

            // Make sure detector is HC
            List<String> categoryFieldsFromResponse = getAdResponse.getDetector().getCategoryFields();
            if (categoryFieldsFromResponse == null || categoryFieldsFromResponse.isEmpty()) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "No category fields found for detector ID %s", request.getId())
                );
            }

            // Validating the category fields. Setting the list to be all category fields,
            // unless otherwise specified
            if (request.getCategoryFields() == null || request.getCategoryFields().isEmpty()) {
                request.setCategoryFields(categoryFieldsFromResponse);
            } else {
                for (String categoryField : request.getCategoryFields()) {
                    if (!categoryFieldsFromResponse.contains(categoryField)) {
                        throw new IllegalArgumentException(
                            String.format(Locale.ROOT, "Category field %s doesn't exist for detector ID %s", categoryField, request.getId())
                        );
                    }
                }
            }

            // Validating historical tasks if historical is true. Setting the ID to the latest historical task's
            // ID, unless otherwise specified
            if (request.getHistorical() == true) {
                ADTask historicalTask = getAdResponse.getHistoricalAdTask();
                if (historicalTask == null) {
                    throw new ResourceNotFoundException(
                        String.format(Locale.ROOT, "No historical tasks found for detector ID %s", request.getId())
                    );
                }
                if (Strings.isNullOrEmpty(request.getTaskId())) {
                    request.setTaskId(historicalTask.getTaskId());
                }
            }

            // Validating the order. If nothing passed use default
            OrderType orderType;
            String orderString = request.getOrder();
            if (Strings.isNullOrEmpty(orderString)) {
                orderType = DEFAULT_ORDER_TYPE;
            } else {
                if (orderString.equals(OrderType.SEVERITY.getName())) {
                    orderType = OrderType.SEVERITY;
                } else if (orderString.equals(OrderType.OCCURRENCE.getName())) {
                    orderType = OrderType.OCCURRENCE;
                } else {
                    // No valid order type was passed, throw an error
                    throw new IllegalArgumentException(String.format(Locale.ROOT, "Ordering by %s is not a valid option", orderString));
                }
            }
            request.setOrder(orderType.getName());

            // Validating the size. If nothing passed use default
            if (request.getSize() == null) {
                request.setSize(DEFAULT_SIZE);
            } else if (request.getSize() > MAX_SIZE) {
                throw new IllegalArgumentException("Size cannot exceed " + MAX_SIZE);
            } else if (request.getSize() <= 0) {
                throw new IllegalArgumentException("Size must be a positive integer");
            }

            // Generating the search request which will contain the generated query
            SearchRequest searchRequest = generateSearchRequest(request);

            // Adding search over any custom result indices
            String rawCustomResultIndexPattern = getAdResponse.getDetector().getCustomResultIndexPattern();
            String customResultIndexPattern = rawCustomResultIndexPattern == null ? null : rawCustomResultIndexPattern.trim();
            if (!Strings.isNullOrEmpty(customResultIndexPattern)) {
                searchRequest.indices(defaultIndex, customResultIndexPattern);
            }

            // Utilizing the existing search() from SearchHandler to handle security permissions. Both user role
            // and backend role filtering is handled in there, and any error will be propagated up and
            // returned as a failure in this Listener.
            // This same method is used for security handling for the search results action. Since this action
            // is doing fundamentally the same thing, we can reuse the security logic here.
            searchHandler
                .search(
                    searchRequest,
                    new TopAnomalyResultListener(
                        listener,
                        searchRequest.source(),
                        clock.millis() + TOP_ANOMALY_RESULT_TIMEOUT_IN_MILLIS,
                        request.getSize(),
                        orderType,
                        customResultIndexPattern
                    )
                );

        }, exception -> {
            logger.error("Failed to get top anomaly results", exception);
            listener.onFailure(exception);
        }));

    }

    /**
     * ActionListener class to handle bucketed search results in a paginated fashion.
     * Note that the bucket_sort aggregation is a pipeline aggregation, and is executed
     * after all non-pipeline aggregations (including the composite bucket aggregation).
     * Because of this, the sorting is only done locally based on the buckets
     * in the current page. To get around this issue, we use a max
     * heap and add all results to the heap until there are no more result buckets,
     * to get the globally sorted set of result buckets.
     */
    class TopAnomalyResultListener implements ActionListener<SearchResponse> {
        private ActionListener<SearchTopAnomalyResultResponse> listener;
        SearchSourceBuilder searchSourceBuilder;
        private long expirationEpochMs;
        private int maxResults;
        private PriorityQueue<AnomalyResultBucket> topResultsHeap;
        private String customResultIndex;

        TopAnomalyResultListener(
            ActionListener<SearchTopAnomalyResultResponse> listener,
            SearchSourceBuilder searchSourceBuilder,
            long expirationEpochMs,
            int maxResults,
            OrderType orderType,
            String customResultIndex
        ) {
            this.listener = listener;
            this.searchSourceBuilder = searchSourceBuilder;
            this.expirationEpochMs = expirationEpochMs;
            this.maxResults = maxResults;
            this.topResultsHeap = new PriorityQueue<>(maxResults, new Comparator<AnomalyResultBucket>() {
                // Sorting by ascending order of anomaly grade or doc count
                @Override
                public int compare(AnomalyResultBucket bucket1, AnomalyResultBucket bucket2) {
                    if (orderType == OrderType.SEVERITY) {
                        return Double.compare(bucket1.getMaxAnomalyGrade(), bucket2.getMaxAnomalyGrade());
                    } else {
                        return Integer.compare(bucket1.getDocCount(), bucket2.getDocCount());
                    }
                }
            });
            this.customResultIndex = customResultIndex;
        }

        @Override
        public void onResponse(SearchResponse response) {
            try {
                Aggregations aggs = response.getAggregations();
                if (aggs == null) {
                    // This would indicate some bug or some opensearch core changes that we are not aware of (we don't keep up-to-date with
                    // the large amounts of changes there). For example, they may change to if there are results return it; otherwise return
                    // null instead of an empty Aggregations as they currently do.
                    logger.warn("Unexpected null aggregation.");
                    listener.onResponse(new SearchTopAnomalyResultResponse(new ArrayList<>()));
                    return;
                }

                Aggregation aggResults = aggs.get(MULTI_BUCKETS_FIELD);
                if (aggResults == null) {
                    listener.onFailure(new IllegalArgumentException("Failed to find valid aggregation result"));
                    return;
                }

                CompositeAggregation compositeAgg = (CompositeAggregation) aggResults;
                List<AnomalyResultBucket> bucketResults = compositeAgg
                    .getBuckets()
                    .stream()
                    .map(bucket -> AnomalyResultBucket.createAnomalyResultBucket(bucket))
                    .collect(Collectors.toList());

                // Add all of the results to the heap, and only keep the top maxResults buckets.
                // Note that the top results heap is implemented as a min heap, so by polling
                // the lowest values from the heap, only the top values remain.
                topResultsHeap.addAll(bucketResults);
                while (topResultsHeap.size() > maxResults) {
                    topResultsHeap.poll();
                }

                // If afterKey is null: we've hit the end of results. Return the results
                Map<String, Object> afterKey = compositeAgg.afterKey();
                if (afterKey == null) {
                    listener.onResponse(new SearchTopAnomalyResultResponse(getDescendingOrderListFromHeap(topResultsHeap)));
                } else if (expirationEpochMs < clock.millis()) {
                    if (topResultsHeap.isEmpty()) {
                        listener.onFailure(new TimeSeriesException("Timed out getting all top anomaly results. Please retry later."));
                    } else {
                        logger.info("Timed out getting all top anomaly results. Sending back partial results.");
                        listener.onResponse(new SearchTopAnomalyResultResponse(getDescendingOrderListFromHeap(topResultsHeap)));
                    }
                } else {
                    CompositeAggregationBuilder aggBuilder = (CompositeAggregationBuilder) searchSourceBuilder
                        .aggregations()
                        .getAggregatorFactories()
                        .iterator()
                        .next();
                    aggBuilder.aggregateAfter(afterKey);

                    // Searching more, using an updated source with an after_key
                    SearchRequest searchRequest = Strings.isNullOrEmpty(customResultIndex)
                        ? new SearchRequest().indices(defaultIndex)
                        : new SearchRequest().indices(defaultIndex, customResultIndex);
                    searchHandler.search(searchRequest.source(searchSourceBuilder), this);
                }

            } catch (Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("Failed to paginate top anomaly results", e);
            listener.onFailure(e);
        }
    }

    /**
     * Generates the entire search request to pass to the search handler
     *
     * @param request the request containing the all of the user-specified parameters needed to generate the request
     * @return the SearchRequest to pass to the SearchHandler
     */
    private SearchRequest generateSearchRequest(SearchTopAnomalyResultRequest request) {
        SearchRequest searchRequest = new SearchRequest().indices(defaultIndex);
        QueryBuilder query = generateQuery(request);
        AggregationBuilder aggregation = generateAggregation(request);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    /**
     * Generates the query with appropriate filters on the results indices.
     * If fetching real-time results:
     * 1) term filter on detector_id
     * 2) must_not filter on task_id (because real-time results don't have a 'task_id' field associated with them in the document)
     * If fetching historical results:
     * 1) term filter on the task_id
     *
     * @param request the request containing the necessary fields to generate the query
     * @return the generated query as a QueryBuilder
     */
    private QueryBuilder generateQuery(SearchTopAnomalyResultRequest request) {
        BoolQueryBuilder query = new BoolQueryBuilder();

        // Adding the date range and anomaly grade filters (needed regardless of real-time or historical)
        RangeQueryBuilder dateRangeFilter = QueryBuilders
            .rangeQuery(CommonName.DATA_END_TIME_FIELD)
            .gte(request.getStartTime().toEpochMilli())
            .lte(request.getEndTime().toEpochMilli());
        RangeQueryBuilder anomalyGradeFilter = QueryBuilders.rangeQuery(AnomalyResult.ANOMALY_GRADE_FIELD).gt(0);
        query.filter(dateRangeFilter).filter(anomalyGradeFilter);

        if (request.getHistorical() == true) {
            TermQueryBuilder taskIdFilter = QueryBuilders.termQuery(CommonName.TASK_ID_FIELD, request.getTaskId());
            query.filter(taskIdFilter);
        } else {
            TermQueryBuilder detectorIdFilter = QueryBuilders.termQuery(AnomalyResult.DETECTOR_ID_FIELD, request.getId());
            ExistsQueryBuilder taskIdExistsFilter = QueryBuilders.existsQuery(CommonName.TASK_ID_FIELD);
            query.filter(detectorIdFilter).mustNot(taskIdExistsFilter);
        }
        return query;
    }

    /**
     * Generates the composite aggregation.
     * Creating a list of sources based on the set of category fields, and sorting on the returned result buckets
     *
     * @param request the request containing the necessary fields to generate the aggregation
     * @return the generated aggregation as an AggregationBuilder
     */
    private AggregationBuilder generateAggregation(SearchTopAnomalyResultRequest request) {
        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        for (String categoryField : request.getCategoryFields()) {
            Script script = QueryUtil.getScriptForCategoryField(categoryField);
            sources.add(new TermsValuesSourceBuilder(categoryField).script(script));
        }

        // Generate the max anomaly grade aggregation
        AggregationBuilder maxAnomalyGradeAggregation = AggregationBuilders
            .max(AnomalyResultBucket.MAX_ANOMALY_GRADE_FIELD)
            .field(AnomalyResult.ANOMALY_GRADE_FIELD);

        // Generate the bucket sort aggregation (depends on order type)
        String sortField = request.getOrder().equals(OrderType.SEVERITY.getName())
            ? AnomalyResultBucket.MAX_ANOMALY_GRADE_FIELD
            : COUNT_FIELD;
        BucketSortPipelineAggregationBuilder bucketSort = PipelineAggregatorBuilders
            .bucketSort(BUCKET_SORT_FIELD, new ArrayList<>(Arrays.asList(new FieldSortBuilder(sortField).order(SortOrder.DESC))));

        return AggregationBuilders
            .composite(MULTI_BUCKETS_FIELD, sources)
            .size(PAGE_SIZE)
            .subAggregation(maxAnomalyGradeAggregation)
            .subAggregation(bucketSort);
    }

    /**
     * Creates a descending-ordered List from a min heap.
     *
     * @param minHeap a min heap
     * @return an ordered List containing all of the elements in the heap
     */
    private List<AnomalyResultBucket> getDescendingOrderListFromHeap(PriorityQueue<AnomalyResultBucket> minHeap) {
        List<AnomalyResultBucket> topResultsHeapAsList = new ArrayList<>();
        while (!minHeap.isEmpty()) {
            topResultsHeapAsList.add(minHeap.poll());
        }
        // Need to reverse the list, since polling from a min heap
        // will return results in ascending order
        Collections.reverse(topResultsHeapAsList);
        return topResultsHeapAsList;
    }

}
