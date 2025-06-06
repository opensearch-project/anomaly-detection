/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.SecurityClientUtil;

public class LatestTimeRetriever {
    public static final Logger logger = LogManager.getLogger(LatestTimeRetriever.class);

    protected static final String AGG_NAME_TOP = "top_agg";

    private final Config config;
    private final AggregationPrep aggregationPrep;
    private final SecurityClientUtil clientUtil;
    private final Client client;
    private final User user;
    private final AnalysisType context;
    private final SearchFeatureDao searchFeatureDao;

    public LatestTimeRetriever(
        Config config,
        TimeValue requestTimeout,
        SecurityClientUtil clientUtil,
        Client client,
        User user,
        AnalysisType context,
        SearchFeatureDao searchFeatureDao
    ) {
        this.config = config;
        this.aggregationPrep = new AggregationPrep(searchFeatureDao, requestTimeout, config);
        this.clientUtil = clientUtil;
        this.client = client;
        this.user = user;
        this.context = context;
        this.searchFeatureDao = searchFeatureDao;
    }

    /**
     * Need to first retrieve latest date time before checking if HC analysis or not.
     * If the config is HC then we will find the top entity and treat as single stream for
     * validation purposes
     * @param listener to return latest time and entity attributes if the config is HC
     */
    public void checkIfHC(ActionListener<Pair<Optional<Long>, Map<String, Object>>> listener) {
        searchFeatureDao.getLatestDataTime(user, config, Optional.empty(), context, ActionListener.wrap(latestTime -> {
            if (latestTime.isEmpty()) {
                listener.onResponse(Pair.of(Optional.empty(), Collections.emptyMap()));
            } else {
                long currentEpochMillis = Instant.now().toEpochMilli();
                long timeRangeEnd = latestTime.get();
                if (currentEpochMillis < timeRangeEnd) {
                    logger.info(new ParameterizedMessage("Future date is detected: [{}]", latestTime.get()));
                }

                if (config.isHighCardinality()) {
                    getTopEntity(listener, timeRangeEnd);
                } else {
                    listener.onResponse(Pair.of(Optional.of(timeRangeEnd), Collections.emptyMap()));
                }
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                listener.onResponse(Pair.of(Optional.empty(), Collections.emptyMap()));
            }
            listener.onFailure(e);
        }));
    }

    // For single category HCs, this method uses bucket aggregation and sort to get the category field
    // that have the highest document count in order to use that top entity for further validation
    // For multi-category HCs we use a composite aggregation to find the top fields for the entity
    // with the highest doc count.
    public void getTopEntity(ActionListener<Pair<Optional<Long>, Map<String, Object>>> topEntityListener, long latestTimeMillis) {
        // Look at data back to the lower bound given the max interval we recommend or one given
        long maxIntervalInMinutes = Math.max(TimeSeriesSettings.MAX_INTERVAL_REC_LENGTH_IN_MINUTES, config.getIntervalInMinutes());
        LongBounds timeRangeBounds = aggregationPrep
            .getTimeRangeBounds(new IntervalTimeConfiguration(maxIntervalInMinutes, ChronoUnit.MINUTES), latestTimeMillis);
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(config.getTimeField())
            .from(timeRangeBounds.getMin())
            .to(timeRangeBounds.getMax());
        AggregationBuilder bucketAggs;
        Map<String, Object> topKeys = new HashMap<>();
        if (config.getCategoryFields().size() == 1) {
            bucketAggs = AggregationBuilders.terms(AGG_NAME_TOP).field(config.getCategoryFields().get(0)).order(BucketOrder.count(true));
        } else {
            bucketAggs = AggregationBuilders
                .composite(
                    AGG_NAME_TOP,
                    config.getCategoryFields().stream().map(f -> new TermsValuesSourceBuilder(f).field(f)).collect(Collectors.toList())
                )
                .size(1000)
                .subAggregation(
                    PipelineAggregatorBuilders
                        .bucketSort("bucketSort", Collections.singletonList(new FieldSortBuilder("_count").order(SortOrder.DESC)))
                        .size(1000)
                );
        }
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(rangeQuery)
            .aggregation(bucketAggs)
            .trackTotalHits(false)
            .size(0);
        SearchRequest searchRequest = new SearchRequest().indices(config.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        final ActionListener<SearchResponse> searchResponseListener = ActionListener.wrap(response -> {
            Aggregations aggs = response.getAggregations();
            if (aggs == null) {
                logger.warn("empty aggregation");
                topEntityListener.onResponse(Pair.of(Optional.empty(), Collections.emptyMap()));
                return;
            }
            if (config.getCategoryFields().size() == 1) {
                Terms entities = aggs.get(AGG_NAME_TOP);

                List<? extends Terms.Bucket> sortedBuckets = entities
                    .getBuckets()
                    .stream()
                    // docCount is typically long in ES, so use comparingLong
                    .sorted(Comparator.comparingLong(Terms.Bucket::getDocCount))
                    .collect(Collectors.toList());

                if (!sortedBuckets.isEmpty()) {
                    int medianIndex = (sortedBuckets.size() - 1) / 2; // For even sizes, picks the lower median
                    Terms.Bucket medianBucket = sortedBuckets.get(medianIndex);
                    String medianKey = medianBucket.getKeyAsString();
                    topKeys.put(config.getCategoryFields().get(0), medianKey);
                } else {
                    topKeys.put(config.getCategoryFields().get(0), null);
                }

            } else {
                CompositeAggregation compositeAgg = aggs.get(AGG_NAME_TOP);

                List<? extends CompositeAggregation.Bucket> sortedCompositeBuckets = compositeAgg
                    .getBuckets()
                    .stream()
                    .sorted(Comparator.comparingLong(CompositeAggregation.Bucket::getDocCount))
                    .collect(Collectors.toList());

                if (!sortedCompositeBuckets.isEmpty()) {
                    int medianIndex = (sortedCompositeBuckets.size() - 1) / 2;
                    CompositeAggregation.Bucket medianBucket = sortedCompositeBuckets.get(medianIndex);

                    // medianBucket.getKey() is already a Map<String, Object> of the category fields
                    topKeys.putAll(medianBucket.getKey());
                } else {
                    // No buckets, so we can either leave topKeys empty or
                    // fill it with null values for each category field
                    config.getCategoryFields().forEach(f -> topKeys.put(f, null));
                }

            }
            for (Map.Entry<String, Object> entry : topKeys.entrySet()) {
                if (entry.getValue() == null) {
                    topEntityListener.onResponse(Pair.of(Optional.empty(), Collections.emptyMap()));
                    return;
                }
            }
            topEntityListener.onResponse(Pair.of(Optional.of(latestTimeMillis), topKeys));
        }, topEntityListener::onFailure);
        // using the original context in listener as user roles have no permissions for internal operations like fetching a
        // checkpoint
        clientUtil
            .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                searchRequest,
                client::search,
                user,
                client,
                context,
                searchResponseListener
            );
    }
}
