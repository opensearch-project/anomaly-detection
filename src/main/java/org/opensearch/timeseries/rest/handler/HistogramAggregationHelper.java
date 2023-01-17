/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.TimeConfiguration;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

/**
 * the class provides helper methods specifically for histogram aggregations
 *
 */
public class HistogramAggregationHelper {
    protected static final Logger logger = LogManager.getLogger(HistogramAggregationHelper.class);

    protected static final String AGGREGATION = "agg";

    private Config config;
    private final TimeValue requestTimeout;

    public HistogramAggregationHelper(Config config, TimeValue requestTimeout) {
        this.config = config;
        this.requestTimeout = requestTimeout;
    }

    public Histogram checkBucketResultErrors(SearchResponse response) {
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

    public AggregationBuilder getBucketAggregation(int intervalInMinutes, LongBounds timeStampBound) {
        return AggregationBuilders
            .dateHistogram(AGGREGATION)
            .field(config.getTimeField())
            .minDocCount(1)
            .hardBounds(timeStampBound)
            .fixedInterval(DateHistogramInterval.minutes(intervalInMinutes));
    }

    public Long timeConfigToMilliSec(TimeConfiguration timeConfig) {
        return Optional.ofNullable((IntervalTimeConfiguration) timeConfig).map(t -> t.toDuration().toMillis()).orElse(0L);
    }

    public LongBounds getTimeRangeBounds(long endMillis, long intervalInMillis) {
        Long startMillis = endMillis - (getNumberOfSamples(intervalInMillis) * intervalInMillis);
        return new LongBounds(startMillis, endMillis);
    }

    public int getNumberOfSamples(long intervalInMillis) {
        return Math
            .max(
                (int) (Duration.ofHours(TimeSeriesSettings.TRAIN_SAMPLE_TIME_RANGE_IN_HOURS).toMillis() / intervalInMillis),
                TimeSeriesSettings.MIN_TRAIN_SAMPLES
            );
    }

    /**
     * @param histogram buckets returned via Date historgram aggregation
     * @param intervalInMillis suggested interval to use
     * @return the number of buckets having data
     */
    public double processBucketAggregationResults(Histogram histogram, long intervalInMillis, Config config) {
        // In all cases, when the specified end time does not exist, the actual end time is the closest available time after the specified
        // end.
        // so we only have non-empty buckets
        List<? extends Bucket> bucketsInResponse = histogram.getBuckets();
        if (bucketsInResponse.size() >= config.getShingleSize() + TimeSeriesSettings.NUM_MIN_SAMPLES) {
            long minTimestampMillis = convertKeyToEpochMillis(bucketsInResponse.get(0).getKey());
            long maxTimestampMillis = convertKeyToEpochMillis(bucketsInResponse.get(bucketsInResponse.size() - 1).getKey());
            double totalBuckets = (maxTimestampMillis - minTimestampMillis) / intervalInMillis;
            return histogram.getBuckets().size() / totalBuckets;
        }
        return 0;
    }

    public SearchSourceBuilder getSearchSourceBuilder(QueryBuilder query, AggregationBuilder aggregation) {
        return new SearchSourceBuilder().query(query).aggregation(aggregation).size(0).timeout(requestTimeout);
    }

    public static long convertKeyToEpochMillis(Object key) {
        return key instanceof ZonedDateTime ? ((ZonedDateTime) key).toInstant().toEpochMilli()
            : key instanceof Double ? ((Double) key).longValue()
            : key instanceof Long ? (Long) key
            : -1L;
    }
}
