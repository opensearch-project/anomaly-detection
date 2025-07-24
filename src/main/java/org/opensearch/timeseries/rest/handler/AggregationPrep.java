/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;

public class AggregationPrep {
    protected static final Logger logger = LogManager.getLogger(AggregationPrep.class);

    private SearchFeatureDao searchFeatureDao;
    private Config config;
    TimeValue requestTimeout;

    public static final String AGGREGATION = "agg";

    public AggregationPrep(SearchFeatureDao searchFeatureDao, TimeValue requestTimeout, Config config) {
        this.searchFeatureDao = searchFeatureDao;
        this.requestTimeout = requestTimeout;
        this.config = config;
    }

    /**
     * Returns the time‑range bounds using this detector’s **default history length**
     * (i.e., the value provided by {@link #getNumberOfSamples()}).
     *
     * <p>The method delegates to
     * {@link #getTimeRangeBounds(IntervalTimeConfiguration, long, int)} and is a
     * convenience overload for callers that do not need to specify a custom
     * sample count.</p>
     *
     * @param interval   sampling interval configuration (e.g., “5m”, “1h”)
     * @param endMillis  exclusive upper bound of the time range, expressed in epoch ms
     * @return           {@code LongBounds} where {@code getMin()} is the computed
     *                   start time and {@code getMax()} equals {@code endMillis}
     */
    public LongBounds getTimeRangeBounds(IntervalTimeConfiguration interval, long endMillis) {
        return getTimeRangeBounds(interval, endMillis, getNumberOfSamples());
    }

    /**
     * Returns the time‑range bounds using an **explicitly supplied history length**.
     *
     * <p>The start time is computed as {@code endMillis − (numberOfSamples × interval)}.
     * Use this overload when the caller wants full control over how many historical
     * samples are considered in the query window.</p>
     *
     * @param interval         sampling interval configuration (e.g., “5m”, “1h”)
     * @param endMillis        exclusive upper bound of the time range, expressed in epoch ms
     * @param numberOfSamples  number of historical samples to include; must be &gt; 0
     * @return                 {@code LongBounds} with {@code getMin()} equal to the
     *                         calculated start time and {@code getMax()} equal to {@code endMillis}
     * @throws IllegalArgumentException if {@code numberOfSamples} is non‑positive
     */
    public LongBounds getTimeRangeBounds(IntervalTimeConfiguration interval, long endMillis, int numberOfSamples) {
        long intervalInMillis = IntervalTimeConfiguration.getIntervalInMinute(interval) * 60000;
        Long startMillis = endMillis - (numberOfSamples * intervalInMillis);
        return new LongBounds(startMillis, endMillis);
    }

    public int getNumberOfSamples() {
        return config.getHistoryIntervals();
    }

    public long getShingleCount(SearchResponse response) {
        // as feature query might contain filter, use feature query as we do in cold start
        if (config.getEnabledFeatureIds() != null && config.getEnabledFeatureIds().size() > 0) {
            return searchFeatureDao.countContinuousShinglesFromDateRangeSearch(response, config);
        } else {
            return searchFeatureDao.countContinuousShinglesFromHistogramSearch(response, config, false);
        }
    }

    public List<Long> getTimestamps(SearchResponse response) {
        if (config.getEnabledFeatureIds() != null && config.getEnabledFeatureIds().size() > 0) {
            return searchFeatureDao.parseColdStartSampleTimestamp(response, false, config);
        } else {
            Histogram aggregate = validateAndRetrieveHistogramAggregation(response);
            // In all cases, when the specified end time does not exist, the actual end time is the closest available time after the
            // specified end.
            // so we only have non-empty buckets
            // in the original order, buckets are sorted in the ascending order of timestamps.
            // Since the stream processing preserves the order of elements, we don't need to sort timestamps again.
            return aggregate
                .getBuckets()
                .stream()
                .map(entry -> AggregationPrep.convertKeyToEpochMillis(entry.getKey()))
                .collect(Collectors.toList());
        }
    }

    public SearchRequest createSearchRequest(
        IntervalTimeConfiguration currentInterval,
        LongBounds currentTimeStampBounds,
        Map<String, Object> topEntity,
        int histogramMinDocCount
    ) {
        if (config.getEnabledFeatureIds() != null && config.getEnabledFeatureIds().size() > 0) {
            List<Entry<Long, Long>> ranges = searchFeatureDao
                .getTrainSampleRanges(
                    currentInterval,
                    currentTimeStampBounds.getMin(),
                    currentTimeStampBounds.getMax(),
                    getNumberOfSamples()
                );
            return searchFeatureDao
                .createColdStartFeatureSearchRequest(
                    config,
                    ranges,
                    topEntity.size() == 0 ? Optional.empty() : Optional.of(Entity.createEntityByReordering(topEntity))
                );
        } else {
            return composeHistogramQuery(
                topEntity,
                (int) IntervalTimeConfiguration.getIntervalInMinute(currentInterval),
                currentTimeStampBounds,
                histogramMinDocCount
            );
        }
    }

    public SearchRequest createSearchRequestForFeature(
        IntervalTimeConfiguration currentInterval,
        LongBounds currentTimeStampBounds,
        Map<String, Object> topEntity,
        int featureIndex
    ) {
        /*
         * Example request:
         {
                "size": 0,
                "query": {
                    "bool": {
                        "filter": [
                            {
                                "match_all": {
                                    "boost": 1
                                }
                            }
                        ],
                        "adjust_pure_negative": true,
                        "boost": 1
                    }
                },
                "aggregations": {
                    "date_range": {
                        "date_range": {
                            "field": "timestamp",
                            "format": "epoch_millis",
                            "ranges": [
                                {
                                    "from": 1714557000000,
                                    "to": 1714557600000
                                },
                                ...
                                {
                                    "from": 1714580400000,
                                    "to": 1714581000000
                                }
                            ],
                            "keyed": false
                        },
                        "aggregations": {
                            "max1": {
                                "filter": {
                                    "bool": {
                                        "must": [
                                            {
                                                "range": {
                                                    "timestamp": {
                                                        "from": null,
                                                        "to": 1714539000000,
                                                        "include_lower": true,
                                                        "include_upper": false,
                                                        "boost": 1
                                                    }
                                                }
                                            }
                                        ],
                                        "adjust_pure_negative": true,
                                        "boost": 1
                                    }
                                },
                                "aggregations": {
                                    "max1": {
                                        "max": {
                                            "field": "visitCount"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
         */
        if (config.getEnabledFeatureIds() != null && config.getEnabledFeatureIds().size() > 0) {
            List<Entry<Long, Long>> ranges = searchFeatureDao
                .getTrainSampleRanges(
                    currentInterval,
                    currentTimeStampBounds.getMin(),
                    currentTimeStampBounds.getMax(),
                    getNumberOfSamples()
                );
            return searchFeatureDao
                .createColdStartFeatureSearchRequestForSingleFeature(
                    config,
                    ranges,
                    topEntity.size() == 0 ? Optional.empty() : Optional.of(Entity.createEntityByReordering(topEntity)),
                    featureIndex
                );
        } else {
            throw new IllegalArgumentException("empty feature");
        }
    }

    public static long convertKeyToEpochMillis(Object key) {
        return key instanceof ZonedDateTime ? ((ZonedDateTime) key).toInstant().toEpochMilli()
            : key instanceof Double ? ((Double) key).longValue()
            : key instanceof Long ? (Long) key
            : -1L;
    }

    public SearchRequest composeHistogramQuery(
        Map<String, Object> topEntity,
        int intervalInMinutes,
        LongBounds timeStampBounds,
        int minimumDocCount
    ) {
        AggregationBuilder aggregation = getHistogramAggregation(intervalInMinutes, timeStampBounds);
        BoolQueryBuilder query = QueryBuilders.boolQuery().filter(config.getFilterQuery());
        if (config.isHighCardinality()) {
            if (topEntity.isEmpty()) {
                throw new ValidationException(
                    CommonMessages.CATEGORY_FIELD_TOO_SPARSE,
                    ValidationIssueType.CATEGORY,
                    ValidationAspect.MODEL
                );
            }
            for (Map.Entry<String, Object> entry : topEntity.entrySet()) {
                query.filter(QueryBuilders.termQuery(entry.getKey(), entry.getValue()));
            }
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(query)
            .aggregation(aggregation)
            .size(minimumDocCount)
            .timeout(requestTimeout);
        return new SearchRequest(config.getIndices().toArray(new String[0])).source(searchSourceBuilder);
    }

    public Histogram validateAndRetrieveHistogramAggregation(SearchResponse response) {
        Aggregations aggs = response.getAggregations();
        if (aggs == null) {
            // This would indicate some bug or some opensearch core changes that we are not aware of (we don't keep up-to-date with
            // the large amounts of changes there). For this reason I'm not throwing a SearchException but instead a validation exception
            // which will be converted to validation response.
            logger.warn("Unexpected null aggregation.");
            throw new ValidationException(
                CommonMessages.MODEL_VALIDATION_FAILED_UNEXPECTEDLY,
                ValidationIssueType.AGGREGATION,
                ValidationAspect.MODEL
            );
        }
        Histogram aggregate = aggs.get(AGGREGATION);
        if (aggregate == null) {
            throw new IllegalArgumentException("Failed to find valid aggregation result");
        }
        return aggregate;
    }

    public AggregationBuilder getHistogramAggregation(int intervalInMinutes, LongBounds timeStampBound) {
        return AggregationBuilders
            .dateHistogram(AggregationPrep.AGGREGATION)
            .field(config.getTimeField())
            .minDocCount(0)
            .hardBounds(timeStampBound)
            .fixedInterval(DateHistogramInterval.minutes(intervalInMinutes));
    }

    public SearchSourceBuilder getSearchSourceBuilder(QueryBuilder query, AggregationBuilder aggregation) {
        return new SearchSourceBuilder().query(query).aggregation(aggregation).size(0).timeout(requestTimeout);
    }

}
