/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler;

import java.io.IOException;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.SecurityClientUtil;

public class IntervalCalculation {
    private final Logger logger = LogManager.getLogger(IntervalCalculation.class);

    private final Config config;
    private final TimeValue requestTimeout;
    private final HistogramAggregationHelper histogramAggHelper;
    private final Client client;
    private final SecurityClientUtil clientUtil;
    private final User user;
    private final AnalysisType context;
    private final Clock clock;
    private final FullBucketRatePredicate acceptanceCriteria;

    public IntervalCalculation(
        Config config,
        TimeValue requestTimeout,
        Client client,
        SecurityClientUtil clientUtil,
        User user,
        AnalysisType context,
        Clock clock
    ) {
        this.config = config;
        this.requestTimeout = requestTimeout;
        this.histogramAggHelper = new HistogramAggregationHelper(config, requestTimeout);
        this.client = client;
        this.clientUtil = clientUtil;
        this.user = user;
        this.context = context;
        this.clock = clock;
        this.acceptanceCriteria = new FullBucketRatePredicate();

    }

    public void findInterval(long latestTime, Map<String, Object> topEntity, ActionListener<IntervalTimeConfiguration> listener) {
        ActionListener<Pair<IntervalTimeConfiguration, Boolean>> minimumIntervalListener = ActionListener.wrap(minIntervalAndValidity -> {
            if (minIntervalAndValidity.getRight()) {
                // the minimum interval is also the interval passing acceptance criteria and we can return immediately
                listener.onResponse(minIntervalAndValidity.getLeft());
            } else if (minIntervalAndValidity.getLeft() == null) {
                // the minimum interval is too large
                listener.onResponse(null);
            } else {
                // starting exploring larger interval
                getBucketAggregates(latestTime, topEntity, minIntervalAndValidity.getLeft(), listener);
            }
        }, listener::onFailure);
        // we use 1 minute = 60000 milliseconds to find minimum interval
        LongBounds longBounds = histogramAggHelper.getTimeRangeBounds(latestTime, 60000);
        findMinimumInterval(topEntity, longBounds, minimumIntervalListener);
    }

    private void getBucketAggregates(
        long latestTime,
        Map<String, Object> topEntity,
        IntervalTimeConfiguration minimumInterval,
        ActionListener<IntervalTimeConfiguration> listener
    ) throws IOException {

        try {
            int newIntervalInMinutes = increaseAndGetNewInterval(minimumInterval);
            LongBounds timeStampBounds = histogramAggHelper.getTimeRangeBounds(latestTime, newIntervalInMinutes);
            SearchRequest searchRequest = composeIntervalQuery(topEntity, newIntervalInMinutes, timeStampBounds);
            ActionListener<IntervalTimeConfiguration> intervalListener = ActionListener
                .wrap(interval -> listener.onResponse(interval), exception -> {
                    listener.onFailure(exception);
                    logger.error("Failed to get interval recommendation", exception);
                });
            final ActionListener<SearchResponse> searchResponseListener = new IntervalRecommendationListener(
                intervalListener,
                searchRequest.source(),
                (IntervalTimeConfiguration) config.getInterval(),
                clock.millis() + TimeSeriesSettings.TOP_VALIDATE_TIMEOUT_IN_MILLIS,
                latestTime,
                timeStampBounds
            );
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
        } catch (ValidationException ex) {
            listener.onFailure(ex);
        }
    }

    /**
     *
     * @param oldInterval
     * @return new interval in minutes
     */
    private int increaseAndGetNewInterval(IntervalTimeConfiguration oldInterval) {
        return (int) Math
            .ceil(
                IntervalTimeConfiguration.getIntervalInMinute(oldInterval)
                    * TimeSeriesSettings.INTERVAL_RECOMMENDATION_INCREASING_MULTIPLIER
            );
    }

    /**
     * ActionListener class to handle execution of multiple bucket aggregations one after the other
     * Bucket aggregation with different interval lengths are executed one by one to check if the data is dense enough
     * We only need to execute the next query if the previous one led to data that is too sparse.
     */
    class IntervalRecommendationListener implements ActionListener<SearchResponse> {
        private final ActionListener<IntervalTimeConfiguration> intervalListener;
        SearchSourceBuilder searchSourceBuilder;
        IntervalTimeConfiguration currentIntervalToTry;
        private final long expirationEpochMs;
        private final long latestTime;
        private LongBounds currentTimeStampBounds;

        IntervalRecommendationListener(
            ActionListener<IntervalTimeConfiguration> intervalListener,
            SearchSourceBuilder searchSourceBuilder,
            IntervalTimeConfiguration currentIntervalToTry,
            long expirationEpochMs,
            long latestTime,
            LongBounds timeStampBounds
        ) {
            this.intervalListener = intervalListener;
            this.searchSourceBuilder = searchSourceBuilder;
            this.currentIntervalToTry = currentIntervalToTry;
            this.expirationEpochMs = expirationEpochMs;
            this.latestTime = latestTime;
            this.currentTimeStampBounds = timeStampBounds;
        }

        @Override
        public void onResponse(SearchResponse response) {
            try {
                Histogram aggregate = null;
                try {
                    aggregate = histogramAggHelper.checkBucketResultErrors(response);
                } catch (ValidationException e) {
                    intervalListener.onFailure(e);
                }

                if (aggregate == null) {
                    intervalListener.onResponse(null);
                    return;
                }

                int newIntervalMinute = increaseAndGetNewInterval(currentIntervalToTry);
                double fullBucketRate = histogramAggHelper.processBucketAggregationResults(aggregate, newIntervalMinute * 60000, config);
                // If rate is above success minimum then return interval suggestion.
                if (fullBucketRate > TimeSeriesSettings.INTERVAL_BUCKET_MINIMUM_SUCCESS_RATE) {
                    intervalListener.onResponse(this.currentIntervalToTry);
                } else if (expirationEpochMs < clock.millis()) {
                    intervalListener
                        .onFailure(
                            new ValidationException(
                                CommonMessages.TIMEOUT_ON_INTERVAL_REC,
                                ValidationIssueType.TIMEOUT,
                                ValidationAspect.MODEL
                            )
                        );
                    logger.info(CommonMessages.TIMEOUT_ON_INTERVAL_REC);
                    // keep trying higher intervals as new interval is below max, and we aren't decreasing yet
                } else if (newIntervalMinute < TimeSeriesSettings.MAX_INTERVAL_REC_LENGTH_IN_MINUTES) {
                    searchWithDifferentInterval(newIntervalMinute);
                    // The below block is executed only the first time when new interval is above max and
                    // we aren't decreasing yet, at this point we will start decreasing for the first time
                    // if we are inside the below block
                } else {
                    // newIntervalMinute >= MAX_INTERVAL_REC_LENGTH_IN_MINUTES
                    intervalListener.onResponse(null);
                }

            } catch (Exception e) {
                onFailure(e);
            }
        }

        private void searchWithDifferentInterval(int newIntervalMinuteValue) {
            this.currentIntervalToTry = new IntervalTimeConfiguration(newIntervalMinuteValue, ChronoUnit.MINUTES);
            this.currentTimeStampBounds = histogramAggHelper.getTimeRangeBounds(latestTime, newIntervalMinuteValue);
            // Searching again using an updated interval
            SearchSourceBuilder updatedSearchSourceBuilder = histogramAggHelper
                .getSearchSourceBuilder(
                    searchSourceBuilder.query(),
                    histogramAggHelper.getBucketAggregation(newIntervalMinuteValue, currentTimeStampBounds)
                );
            // using the original context in listener as user roles have no permissions for internal operations like fetching a
            // checkpoint
            clientUtil
                .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                    new SearchRequest().indices(config.getIndices().toArray(new String[0])).source(updatedSearchSourceBuilder),
                    client::search,
                    user,
                    client,
                    context,
                    this
                );
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("Failed to recommend new interval", e);
            intervalListener
                .onFailure(
                    new ValidationException(
                        CommonMessages.MODEL_VALIDATION_FAILED_UNEXPECTEDLY,
                        ValidationIssueType.AGGREGATION,
                        ValidationAspect.MODEL
                    )
                );
        }
    }

    /**
     * This method calculates median timestamp difference as minimum interval.
     *
     *
     * Using the median timestamp difference as a minimum sampling interval is a heuristic approach
     * that can be beneficial in specific contexts, especially when dealing with irregularly spaced data.
     *
     * Advantages:
     * 1. Robustness: The median is less sensitive to outliers compared to the mean. This makes it a
     *    more stable metric in the presence of irregular data points or anomalies.
     * 2. Reflects Typical Intervals: The median provides a measure of the "typical" interval between
     *    data points, which can be useful when there are varying intervals.
     *
     * Disadvantages:
     * 1. Not Standard in Signal Processing: Traditional signal processing often relies on fixed
     *    sampling rates determined by the Nyquist-Shannon sampling theorem. The median-based approach
     *    is more of a data-driven heuristic.
     * 2. May Not Capture All Features: Depending on the nature of the data, using the median interval
     *    might miss some rapid events or features in the data.
     *
     * In summary, while not a standard practice, using the median timestamp difference as a sampling
     * interval can be a practical approach in scenarios where data arrival is irregular and there's
     * a need to balance between capturing data features and avoiding over-sampling.
     *
     * @param topEntity top entity to use
     * @param timeStampBounds Used to determine start and end date range to search for data
     * @param listener returns minimum interval and whether the interval passes data density test
     */
    private void findMinimumInterval(
        Map<String, Object> topEntity,
        LongBounds timeStampBounds,
        ActionListener<Pair<IntervalTimeConfiguration, Boolean>> listener
    ) {
        try {
            SearchRequest searchRequest = composeIntervalQuery(topEntity, 1, timeStampBounds);
            final ActionListener<SearchResponse> searchResponseListener = ActionListener.wrap(response -> {
                Histogram aggregate = null;
                try {
                    aggregate = histogramAggHelper.checkBucketResultErrors(response);
                } catch (ValidationException e) {
                    listener.onFailure(e);
                }

                if (aggregate == null) {
                    // fail to find the minimum interval. Return one minute.
                    logger.warn("Fail to get aggregated result");
                    listener.onResponse(Pair.of(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES), Boolean.FALSE));
                    return;
                }
                // In all cases, when the specified end time does not exist, the actual end time is the closest available time after the
                // specified end.
                // so we only have non-empty buckets
                // in the original order, buckets are sorted in the ascending order of timestamps.
                // Since the stream processing preserves the order of elements, we don't need to sort timestamps again.
                List<Long> timestamps = aggregate
                    .getBuckets()
                    .stream()
                    .map(entry -> HistogramAggregationHelper.convertKeyToEpochMillis(entry.getKey()))
                    .collect(Collectors.toList());

                if (timestamps.isEmpty()) {
                    logger.warn("empty data, return one minute by default");
                    listener.onResponse(Pair.of(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES), Boolean.FALSE));
                    return;
                }

                double medianDifference = calculateMedianDifference(timestamps);
                long minimumMinutes = millisecondsToCeilMinutes(((Double) medianDifference).longValue());
                if (minimumMinutes > TimeSeriesSettings.MAX_INTERVAL_REC_LENGTH_IN_MINUTES) {
                    logger.warn("The minimum interval is too large: {}", minimumMinutes);
                    listener.onResponse(Pair.of(null, false));
                    return;
                }
                listener
                    .onResponse(
                        Pair
                            .of(
                                new IntervalTimeConfiguration(minimumMinutes, ChronoUnit.MINUTES),
                                acceptanceCriteria.test(aggregate, minimumMinutes)
                            )
                    );
            }, listener::onFailure);
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
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static double calculateMedianDifference(List<Long> timestamps) {
        List<Long> differences = new ArrayList<>();

        for (int i = 1; i < timestamps.size(); i++) {
            differences.add(timestamps.get(i) - timestamps.get(i - 1));
        }

        Collections.sort(differences);

        int middle = differences.size() / 2;
        if (differences.size() % 2 == 0) {
            // If even number of differences, return the average of the two middle values
            return (differences.get(middle - 1) + differences.get(middle)) / 2.0;
        } else {
            // If odd number of differences, return the middle value
            return differences.get(middle);
        }
    }

    /**
     * Convert a duration in milliseconds to the nearest minute value that is greater than
     * or equal to the given duration.
     *
     * For example, a duration of 123456 milliseconds is slightly more than 2 minutes.
     * So, it gets rounded up and the method returns 3.
     *
     * @param milliseconds The duration in milliseconds.
     * @return The rounded up value in minutes.
     */
    private static long millisecondsToCeilMinutes(long milliseconds) {
        // Since there are 60,000 milliseconds in a minute, we divide by 60,000 to get
        // the number of complete minutes. We add 59,999 before division to ensure
        // that any duration that exceeds a whole minute but is less than the next
        // whole minute is rounded up to the next minute.
        return (milliseconds + 59999) / 60000;
    }

    private SearchRequest composeIntervalQuery(Map<String, Object> topEntity, int intervalInMinutes, LongBounds timeStampBounds) {
        AggregationBuilder aggregation = histogramAggHelper.getBucketAggregation(intervalInMinutes, timeStampBounds);
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
            .size(0)
            .timeout(requestTimeout);
        return new SearchRequest(config.getIndices().toArray(new String[0])).source(searchSourceBuilder);
    }

    interface HistogramPredicate {
        boolean test(Histogram histogram, long minimumMinutes);
    }

    class FullBucketRatePredicate implements HistogramPredicate {

        @Override
        public boolean test(Histogram histogram, long minimumMinutes) {
            double fullBucketRate = histogramAggHelper.processBucketAggregationResults(histogram, minimumMinutes * 60000, config);
            // If rate is above success minimum then return true.
            return fullBucketRate > TimeSeriesSettings.INTERVAL_BUCKET_MINIMUM_SUCCESS_RATE;
        }

    }
}
