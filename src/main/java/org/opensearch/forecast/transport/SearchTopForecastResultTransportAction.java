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

package org.opensearch.forecast.transport;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.forecast.indices.ForecastIndexManagement.ALL_FORECAST_RESULTS_INDEX_PATTERN;
import static org.opensearch.timeseries.util.RestHandlerUtils.createXContentParserFromRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.routing.Preference;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.model.FilterBy;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastResultBucket;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.model.Order;
import org.opensearch.forecast.model.Subaggregation;
import org.opensearch.forecast.transport.handler.ForecastSearchHandler;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.transport.GetConfigRequest;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.QueryUtil;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Transport action to fetch top forecast results for HC forecaster.
 */
public class SearchTopForecastResultTransportAction extends
    HandledTransportAction<SearchTopForecastResultRequest, SearchTopForecastResultResponse> {
    private static final Logger logger = LogManager.getLogger(SearchTopForecastResultTransportAction.class);
    private ForecastSearchHandler searchHandler;
    // Number of buckets to return per page
    private static final String defaultIndex = ALL_FORECAST_RESULTS_INDEX_PATTERN;

    private static final int DEFAULT_SIZE = 5;
    private static final int MAX_SIZE = 50;

    protected static final String AGG_NAME_TERM = "term_agg";

    private final Client client;
    private NamedXContentRegistry xContent;

    @Inject
    public SearchTopForecastResultTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ForecastSearchHandler searchHandler,
        Client client,
        NamedXContentRegistry xContent
    ) {
        super(SearchTopForecastResultAction.NAME, transportService, actionFilters, SearchTopForecastResultRequest::new);
        this.searchHandler = searchHandler;
        this.client = client;
        this.xContent = xContent;
    }

    @Override
    protected void doExecute(Task task, SearchTopForecastResultRequest request, ActionListener<SearchTopForecastResultResponse> listener) {
        GetConfigRequest getForecasterRequest = new GetConfigRequest(
            request.getForecasterId(),
            // The default version value used in
            // org.opensearch.rest.action.RestActions.parseVersion()
            -3L,
            false,
            true,
            "",
            "",
            false,
            null
        );

        client.execute(GetForecasterAction.INSTANCE, getForecasterRequest, ActionListener.wrap(getForecasterResponse -> {
            // Make sure forecaster exists
            if (getForecasterResponse.getForecaster() == null) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "No forecaster found with ID %s", request.getForecasterId()));
            }

            Forecaster forecaster = getForecasterResponse.getForecaster();
            // Make sure forecaster is HC
            List<String> categoryFields = forecaster.getCategoryFields();
            if (categoryFields == null || categoryFields.isEmpty()) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "No category fields found for forecaster ID %s", request.getForecasterId())
                );
            }

            // Validating the category fields. Setting the list to be all category fields,
            // unless otherwise specified
            if (request.getSplitBy() == null || request.getSplitBy().isEmpty()) {
                request.setSplitBy(categoryFields);
            } else {
                for (String categoryField : request.getSplitBy()) {
                    if (!categoryFields.contains(categoryField)) {
                        throw new IllegalArgumentException(
                            String
                                .format(
                                    Locale.ROOT,
                                    "Category field %s doesn't exist for forecaster ID %s",
                                    categoryField,
                                    request.getForecasterId()
                                )
                        );
                    }
                }
            }

            // Validating run once tasks if runOnce is true. Setting the task id to the
            // latest run once task's ID, unless otherwise specified
            if (request.isRunOnce() == true && Strings.isNullOrEmpty(request.getTaskId())) {
                ForecastTask runOnceTask = getForecasterResponse.getRunOnceTask();
                if (runOnceTask == null) {
                    throw new ResourceNotFoundException(
                        String.format(Locale.ROOT, "No latest run once tasks found for forecaster ID %s", request.getForecasterId())
                    );
                }
                request.setTaskId(runOnceTask.getTaskId());
            }

            // Validating the size. If nothing passed use default
            if (request.getSize() == null) {
                request.setSize(DEFAULT_SIZE);
            } else if (request.getSize() > MAX_SIZE) {
                throw new IllegalArgumentException("Size cannot exceed " + MAX_SIZE);
            } else if (request.getSize() <= 0) {
                throw new IllegalArgumentException("Size must be a positive integer");
            }

            // Generating the search request which will contain the generated query
            SearchRequest searchRequest = generateQuery(request, forecaster);

            // Adding search over any custom result indices
            if (!Strings.isNullOrEmpty(forecaster.getCustomResultIndexPattern())) {
                searchRequest.indices(forecaster.getCustomResultIndexPattern());
            }
            // Utilizing the existing search() from SearchHandler to handle security
            // permissions. Both user role
            // and backend role filtering is handled in there, and any error will be
            // propagated up and
            // returned as a failure in this Listener.
            // This same method is used for security handling for the search results action.
            // Since this action
            // is doing fundamentally the same thing, we can reuse the security logic here.
            searchHandler.search(searchRequest, onSearchResponse(request, categoryFields, forecaster, listener));
        }, exception -> {
            logger.error("Failed to get top forecast results", exception);
            listener.onFailure(exception);
        }));

    }

    private ActionListener<SearchResponse> onSearchResponse(
        SearchTopForecastResultRequest request,
        List<String> categoryFields,
        Forecaster forecaster,
        ActionListener<SearchTopForecastResultResponse> listener
    ) {
        return ActionListener.wrap(response -> {
            Aggregations aggs = response.getAggregations();
            if (aggs == null) {
                // empty result (e.g., cannot find forecasts within [forecast from, forecast from + horizon * interval] range).
                listener.onResponse(new SearchTopForecastResultResponse(new ArrayList<>()));
                return;
            }

            Aggregation aggResults = aggs.get(AGG_NAME_TERM);
            if (aggResults == null) {
                // empty result
                listener.onResponse(new SearchTopForecastResultResponse(new ArrayList<>()));
                return;
            }

            List<? extends MultiBucketsAggregation.Bucket> buckets = ((MultiBucketsAggregation) aggResults).getBuckets();
            if (buckets == null || buckets.size() == 0) {
                // empty result
                listener
                    .onFailure(
                        new ResourceNotFoundException(
                            "No forecast value found. forecast_from timestamp or other parameters might be incorrect."
                        )
                    );
                return;
            }

            final GroupedActionListener<ForecastResultBucket> groupListeneer = new GroupedActionListener<>(ActionListener.wrap(r -> {
                // Keep original bucket order
                // Sort the collection based on getBucketIndex() in ascending order
                // and convert it to a List
                List<ForecastResultBucket> sortedList = r
                    .stream()
                    .sorted((a, b) -> Integer.compare(a.getBucketIndex(), b.getBucketIndex()))
                    .collect(Collectors.toList());
                listener.onResponse(new SearchTopForecastResultResponse(new ArrayList<>(sortedList)));
            }, exception -> {
                logger.warn("Failed to find valid aggregation result", exception);
                listener
                    .onFailure(new OpenSearchStatusException("Failed to find valid aggregation result", RestStatus.INTERNAL_SERVER_ERROR));
            }), buckets.size());

            for (int i = 0; i < buckets.size(); i++) {
                MultiBucketsAggregation.Bucket bucket = buckets.get(i);
                createForecastResultBucket(bucket, i, request, categoryFields, forecaster, groupListeneer);
            }
        }, e -> listener.onFailure(e));
    }

    public void createForecastResultBucket(
        MultiBucketsAggregation.Bucket bucket,
        int bucketIndex,
        SearchTopForecastResultRequest request,
        List<String> categoryFields,
        Forecaster forecaster,
        ActionListener<ForecastResultBucket> listener
    ) {
        Map<String, Double> aggregationsMap = new HashMap<>();
        for (Aggregation aggregation : bucket.getAggregations()) {
            if (!(aggregation instanceof NumericMetricsAggregation.SingleValue)) {
                listener
                    .onFailure(
                        new IllegalArgumentException(
                            String.format(Locale.ROOT, "A single value aggregation is required; received [{}]", aggregation)
                        )
                    );
            }
            NumericMetricsAggregation.SingleValue singleValueAggregation = (NumericMetricsAggregation.SingleValue) aggregation;
            aggregationsMap.put(aggregation.getName(), singleValueAggregation.value());
        }
        if (bucket instanceof Terms.Bucket) {
            // our terms key is string
            convertToCategoricalFieldValuePair(
                (String) bucket.getKey(),
                bucketIndex,
                (int) bucket.getDocCount(),
                aggregationsMap,
                request,
                categoryFields,
                forecaster,
                listener
            );
        } else {
            listener
                .onFailure(
                    new IllegalArgumentException(String.format(Locale.ROOT, "We only use terms aggregation in top, but got %s", bucket))
                );
        }
    }

    private void convertToCategoricalFieldValuePair(
        String keyInSearchResponse,
        int bucketIndex,
        int docCount,
        Map<String, Double> aggregations,
        SearchTopForecastResultRequest request,
        List<String> categoryFields,
        Forecaster forecaster,
        ActionListener<ForecastResultBucket> listener
    ) {
        List<String> splitBy = request.getSplitBy();
        Map<String, Object> keys = new HashMap<>();
        // TODO: we only support two categorical fields. Expand to support more categorical fields
        if (splitBy == null || splitBy.size() == categoryFields.size()) {
            // use all categorical fields in splitBy. Convert entity id to concrete attributes.
            findMatchingCategoricalFieldValuePair(keyInSearchResponse, docCount, aggregations, bucketIndex, forecaster, listener);
        } else {
            keys.put(splitBy.get(0), keyInSearchResponse);
            listener.onResponse(new ForecastResultBucket(keys, docCount, aggregations, bucketIndex));
        }
    }

    private void findMatchingCategoricalFieldValuePair(
        String entityId,
        int docCount,
        Map<String, Double> aggregations,
        int bucketIndex,
        Forecaster forecaster,
        ActionListener<ForecastResultBucket> listener
    ) {
        TermQueryBuilder entityIdFilter = QueryBuilders.termQuery(CommonName.ENTITY_ID_FIELD, entityId);

        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().filter(entityIdFilter);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(internalFilterQuery).size(1);

        String resultIndex = Strings.isNullOrEmpty(forecaster.getCustomResultIndexOrAlias())
            ? defaultIndex
            : forecaster.getCustomResultIndexPattern();
        SearchRequest searchRequest = new SearchRequest()
            .indices(resultIndex)
            .source(searchSourceBuilder)
            .preference(Preference.LOCAL.toString());

        String failure = String.format(Locale.ROOT, "Cannot find a result matching entity id %s", entityId);

        final ActionListener<SearchResponse> searchResponseListener = ActionListener.wrap(searchResponse -> {
            try {
                SearchHit[] hits = searchResponse.getHits().getHits();
                if (hits.length == 0) {
                    listener.onFailure(new IllegalArgumentException(failure));
                    return;
                }
                SearchHit searchHit = hits[0];
                try (XContentParser parser = createXContentParserFromRegistry(xContent, searchHit.getSourceRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    Optional<Entity> entity = ForecastResult.parse(parser).getEntity();
                    if (entity.isEmpty()) {
                        listener.onFailure(new IllegalArgumentException(failure));
                        return;
                    }

                    listener
                        .onResponse(
                            new ForecastResultBucket(convertMap(entity.get().getAttributes()), docCount, aggregations, bucketIndex)
                        );
                } catch (Exception e) {
                    listener.onFailure(new IllegalArgumentException(failure, e));
                }
            } catch (Exception e) {
                listener.onFailure(new IllegalArgumentException(failure, e));
            }
        }, e -> listener.onFailure(new IllegalArgumentException(failure, e)));

        searchHandler.search(searchRequest, searchResponseListener);
    }

    private Map<String, Object> convertMap(Map<String, String> stringMap) {
        // Create a new Map<String, Object> and copy the entries
        Map<String, Object> objectMap = new HashMap<>();
        for (Map.Entry<String, String> entry : stringMap.entrySet()) {
            objectMap.put(entry.getKey(), entry.getValue());
        }
        return objectMap;
    }

    /**
     * Generates the entire search request to pass to the search handler
     *
     * @param request the request containing the all of the user-specified
     *                parameters needed to generate the request
     * @param forecaster Forecaster config
     * @return the SearchRequest to pass to the SearchHandler
     */
    private SearchRequest generateQuery(SearchTopForecastResultRequest request, Forecaster forecaster) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        QueryBuilder rangeQuery = generateDateFilter(request, forecaster);
        boolQueryBuilder = boolQueryBuilder.filter(rangeQuery);

        // we only look for documents containing forecasts
        boolQueryBuilder.filter(new ExistsQueryBuilder(ForecastResult.VALUE_FIELD));

        FilterBy filterBy = request.getFilterBy();
        switch (filterBy) {
            case CUSTOM_QUERY:
                if (request.getFilterQuery() != null) {
                    boolQueryBuilder = boolQueryBuilder.filter(request.getFilterQuery());
                }
                break;
            case BUILD_IN_QUERY:
                QueryBuilder buildInSubFilter = generateBuildInSubFilter(request, forecaster);
                if (buildInSubFilter != null) {
                    boolQueryBuilder = boolQueryBuilder.filter(buildInSubFilter);
                }
                break;
            default:
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Unexpected filter by %s", request.getFilterBy()));
        }

        boolQueryBuilder = generateTaskIdFilter(request, boolQueryBuilder);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder).trackTotalHits(false).size(0);

        AggregationBuilder termsAgg = generateTermsAggregation(request, forecaster);
        if (termsAgg != null) {
            searchSourceBuilder = searchSourceBuilder.aggregation(termsAgg);
        }
        return new SearchRequest().indices(defaultIndex).source(searchSourceBuilder);
    }

    private QueryBuilder generateBuildInSubFilter(SearchTopForecastResultRequest request, Forecaster forecaster) {
        BuildInQuery buildInQuery = request.getBuildInQuery();
        switch (buildInQuery) {
            case MIN_CONFIDENCE_INTERVAL_WIDTH:
            case MAX_CONFIDENCE_INTERVAL_WIDTH:
                // Include only documents where horizon_index is configured horizon (indicating the "latest" forecast).
                return QueryBuilders.termQuery(ForecastResult.HORIZON_INDEX_FIELD, forecaster.getHorizon());
            case DISTANCE_TO_THRESHOLD_VALUE:
                RangeQueryBuilder res = QueryBuilders.rangeQuery(ForecastResult.VALUE_FIELD);
                Float threshold = request.getThreshold();
                switch (request.getRelationToThreshold()) {
                    case GREATER_THAN:
                        res = res.gt(threshold);
                        break;
                    case GREATER_THAN_OR_EQUAL_TO:
                        res = res.gte(threshold);
                        break;
                    case LESS_THAN:
                        res = res.lt(threshold);
                        break;
                    case LESS_THAN_OR_EQUAL_TO:
                        res = res.lte(threshold);
                        break;
                }
                return res;
            default:
                // no need to generate filter in cases like MIN_VALUE_WITHIN_THE_HORIZON
                return null;
        }
    }

    /**
     * Adding the date filter (needed regardless of filter by type)
     * @param request top forecaster request
     * @return filter for date
     */
    private RangeQueryBuilder generateDateFilter(SearchTopForecastResultRequest request, Forecaster forecaster) {
        // forecast from is data end time for forecast
        long startInclusive = request.getForecastFrom().toEpochMilli();
        long endExclusive = startInclusive + forecaster.getIntervalInMilliseconds();
        return QueryBuilders.rangeQuery(CommonName.DATA_END_TIME_FIELD).gte(startInclusive).lt(endExclusive);
    }

    /**
     * Generates the query with appropriate filters on the results indices. If
     * fetching real-time results: must_not filter on task_id (because real-time
     * results don't have a 'task_id' field associated with them in the document).
     * If fetching historical results: term filter on the task_id.
     *
     * @param request the request containing the necessary fields to generate the query
     * @param query Bool query to generate
     * @return input bool query with added id related filter
     */
    private BoolQueryBuilder generateTaskIdFilter(SearchTopForecastResultRequest request, BoolQueryBuilder query) {
        if (!Strings.isNullOrEmpty(request.getTaskId())) {
            query.filter(QueryBuilders.termQuery(CommonName.TASK_ID_FIELD, request.getTaskId()));
        } else {
            TermQueryBuilder forecasterIdFilter = QueryBuilders.termQuery(ForecastCommonName.FORECASTER_ID_KEY, request.getForecasterId());
            ExistsQueryBuilder taskIdExistsFilter = QueryBuilders.existsQuery(CommonName.TASK_ID_FIELD);
            query.filter(forecasterIdFilter).mustNot(taskIdExistsFilter);
        }
        return query;
    }

    /**
     * Generates aggregation. Creating a list of sources based on the
     * set of category fields, and sorting on the returned result buckets
     *
     * @param request the request containing the necessary fields to generate the
     *                aggregation
     * @return the generated aggregation as an AggregationBuilder
     */
    private TermsAggregationBuilder generateTermsAggregation(SearchTopForecastResultRequest request, Forecaster forecaster) {
        // TODO: use multi_terms or composite when multiple categorical fields are required.
        // Right now, since we only support two categorical fields, we either use terms
        // aggregation for one categorical field or terms aggregation on entity_id for
        // all categorical fields.
        TermsAggregationBuilder termsAgg = AggregationBuilders.terms(AGG_NAME_TERM).size(request.getSize());

        if (request.getSplitBy().size() == forecaster.getCategoryFields().size()) {
            termsAgg = termsAgg.field(CommonName.ENTITY_ID_FIELD);
        } else if (request.getSplitBy().size() == 1) {
            termsAgg = termsAgg.script(QueryUtil.getScriptForCategoryField(request.getSplitBy().get(0)));
        }

        List<BucketOrder> orders = new ArrayList<>();

        FilterBy filterBy = request.getFilterBy();
        switch (filterBy) {
            case BUILD_IN_QUERY:
                Pair<AggregationBuilder, BucketOrder> aggregationOrderPair = generateBuildInSubAggregation(request);
                termsAgg.subAggregation(aggregationOrderPair.getLeft());
                orders.add(aggregationOrderPair.getRight());
                break;
            case CUSTOM_QUERY:
                // if customers defined customized aggregation
                for (Subaggregation subaggregation : request.getSubaggregations()) {
                    AggregatorFactories.Builder internalAgg;
                    try {
                        internalAgg = ParseUtils.parseAggregators(subaggregation.getAggregation().toString(), xContent, null);
                        AggregationBuilder aggregation = internalAgg.getAggregatorFactories().iterator().next();
                        termsAgg.subAggregation(aggregation);
                        orders.add(BucketOrder.aggregation(aggregation.getName(), subaggregation.getOrder() == Order.ASC ? true : false));
                    } catch (IOException e) {
                        throw new IllegalArgumentException(
                            String.format(Locale.ROOT, "Unexpected IOException when parsing %s", subaggregation),
                            e
                        );
                    }
                }
                break;
            default:
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Unexpected filter by %s", filterBy));
        }

        if (orders.isEmpty()) {
            throw new IllegalArgumentException("Cannot have empty order list");
        }

        termsAgg.order(orders);

        return termsAgg;
    }

    private Pair<AggregationBuilder, BucketOrder> generateBuildInSubAggregation(SearchTopForecastResultRequest request) {
        String aggregationName = null;
        AggregationBuilder aggregation = null;
        BucketOrder order = null;
        BuildInQuery buildInQuery = request.getBuildInQuery();
        switch (buildInQuery) {
            case MIN_CONFIDENCE_INTERVAL_WIDTH:
                aggregationName = BuildInQuery.MIN_CONFIDENCE_INTERVAL_WIDTH.name();
                aggregation = AggregationBuilders.min(aggregationName).field(ForecastResult.INTERVAL_WIDTH_FIELD);
                order = BucketOrder.aggregation(aggregationName, true);
                return Pair.of(aggregation, order);
            case MAX_CONFIDENCE_INTERVAL_WIDTH:
                aggregationName = BuildInQuery.MAX_CONFIDENCE_INTERVAL_WIDTH.name();
                aggregation = AggregationBuilders.max(aggregationName).field(ForecastResult.INTERVAL_WIDTH_FIELD);
                order = BucketOrder.aggregation(aggregationName, false);
                return Pair.of(aggregation, order);
            case MIN_VALUE_WITHIN_THE_HORIZON:
                aggregationName = BuildInQuery.MIN_VALUE_WITHIN_THE_HORIZON.name();
                aggregation = AggregationBuilders.min(aggregationName).field(ForecastResult.VALUE_FIELD);
                order = BucketOrder.aggregation(aggregationName, true);
                return Pair.of(aggregation, order);
            case MAX_VALUE_WITHIN_THE_HORIZON:
                aggregationName = BuildInQuery.MAX_VALUE_WITHIN_THE_HORIZON.name();
                aggregation = AggregationBuilders.max(aggregationName).field(ForecastResult.VALUE_FIELD);
                order = BucketOrder.aggregation(aggregationName, false);
                return Pair.of(aggregation, order);
            case DISTANCE_TO_THRESHOLD_VALUE:
                RelationalOperation relationToThreshold = request.getRelationToThreshold();
                switch (relationToThreshold) {
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL_TO:
                        aggregationName = BuildInQuery.DISTANCE_TO_THRESHOLD_VALUE.name();
                        aggregation = AggregationBuilders.max(aggregationName).field(ForecastResult.VALUE_FIELD);
                        order = BucketOrder.aggregation(aggregationName, false);
                        return Pair.of(aggregation, order);
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL_TO:
                        aggregationName = BuildInQuery.DISTANCE_TO_THRESHOLD_VALUE.name();
                        aggregation = AggregationBuilders.min(aggregationName).field(ForecastResult.VALUE_FIELD);
                        order = BucketOrder.aggregation(aggregationName, true);
                        return Pair.of(aggregation, order);
                    default:
                        throw new IllegalArgumentException(
                            String.format(Locale.ROOT, "Unexpected relation to threshold %s", relationToThreshold)
                        );
                }
            default:
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Unexpected build in query type %s", buildInQuery));
        }
    }
}
