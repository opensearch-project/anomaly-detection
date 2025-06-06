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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.SecurityClientUtil;

public class IntervalCalculation {
    private final Logger logger = LogManager.getLogger(IntervalCalculation.class);

    private final AggregationPrep aggregationPrep;
    private final Client client;
    private final SecurityClientUtil clientUtil;
    private final User user;
    private final AnalysisType context;
    private final Clock clock;
    private final Map<String, Object> topEntity;
    private final long endMillis;

    public IntervalCalculation(
        Config config,
        TimeValue requestTimeout,
        Client client,
        SecurityClientUtil clientUtil,
        User user,
        AnalysisType context,
        Clock clock,
        SearchFeatureDao searchFeatureDao,
        long latestTime,
        Map<String, Object> topEntity
    ) {
        this.aggregationPrep = new AggregationPrep(searchFeatureDao, requestTimeout, config);
        this.client = client;
        this.clientUtil = clientUtil;
        this.user = user;
        this.context = context;
        this.clock = clock;
        this.topEntity = topEntity;
        this.endMillis = latestTime;
    }

    public void findInterval(ActionListener<IntervalTimeConfiguration> listener) {
        ActionListener<IntervalTimeConfiguration> minimumIntervalListener = ActionListener.wrap(minInterval -> {
            if (minInterval == null) {
                // the minimum interval is too large
                listener.onResponse(null);
            } else {
                // starting exploring whether minimum or larger interval satisfy density requirement
                getBucketAggregates(minInterval, listener);
            }
        }, listener::onFailure);
        // we use 1 minute = 60000 milliseconds to find minimum interval
        LongBounds longBounds = aggregationPrep.getTimeRangeBounds(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES), endMillis);
        findMinimumInterval(longBounds, minimumIntervalListener);
    }

    private void getBucketAggregates(IntervalTimeConfiguration minimumInterval, ActionListener<IntervalTimeConfiguration> listener)
        throws IOException {

        try {
            LongBounds timeStampBounds = aggregationPrep.getTimeRangeBounds(minimumInterval, endMillis);
            SearchRequest searchRequest = aggregationPrep.createSearchRequest(minimumInterval, timeStampBounds, topEntity, 0);
            ActionListener<IntervalTimeConfiguration> intervalListener = ActionListener
                .wrap(interval -> listener.onResponse(interval), exception -> {
                    listener.onFailure(exception);
                    logger.error("Failed to get interval recommendation", exception);
                });
            final ActionListener<SearchResponse> searchResponseListener = new IntervalRecommendationListener(
                intervalListener,
                minimumInterval,
                clock.millis() + TimeSeriesSettings.TOP_VALIDATE_TIMEOUT_IN_MILLIS,
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
    public class IntervalRecommendationListener implements ActionListener<SearchResponse> {
        private final ActionListener<IntervalTimeConfiguration> intervalListener;
        IntervalTimeConfiguration currentIntervalToTry;
        private final long expirationEpochMs;
        private LongBounds currentTimeStampBounds;

        public IntervalRecommendationListener(
            ActionListener<IntervalTimeConfiguration> intervalListener,
            IntervalTimeConfiguration currentIntervalToTry,
            long expirationEpochMs,
            LongBounds timeStampBounds
        ) {
            this.intervalListener = intervalListener;
            this.currentIntervalToTry = currentIntervalToTry;
            this.expirationEpochMs = expirationEpochMs;
            this.currentTimeStampBounds = timeStampBounds;
        }

        @Override
        public void onResponse(SearchResponse response) {
            try {
                int newIntervalMinute = increaseAndGetNewInterval(currentIntervalToTry);
                long shingleCount = aggregationPrep.getShingleCount(response);
                // If rate is above success minimum then return interval suggestion.
                if (shingleCount >= TimeSeriesSettings.NUM_MIN_SAMPLES) {
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
            this.currentTimeStampBounds = aggregationPrep.getTimeRangeBounds(currentIntervalToTry, endMillis);
            // using the original context in listener as user roles have no permissions for internal operations like fetching a
            // checkpoint
            clientUtil
                .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                    aggregationPrep.createSearchRequest(currentIntervalToTry, currentTimeStampBounds, topEntity, 0),
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
     * @param timeStampBounds Used to determine start and end date range to search for data
     * @param listener returns minimum interval
     */
    private void findMinimumInterval(LongBounds timeStampBounds, ActionListener<IntervalTimeConfiguration> listener) {
        try {
            // since we only count minimum interval to start exploring, it is filter by at least one doc per bucket.
            // later, we will use minimum doc count to 0 to consider shingle (continuous window)
            SearchRequest searchRequest = aggregationPrep
                .createSearchRequest(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES), timeStampBounds, topEntity, 1);
            final ActionListener<SearchResponse> searchResponseListener = ActionListener.wrap(response -> {
                List<Long> timestamps = aggregationPrep.getTimestamps(response);
                if (timestamps.size() < 2) {
                    // to calculate the difference we need at least 2 timestamps
                    logger.warn("not enough data, return one minute by default");
                    // listener.onResponse(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES));
                    listener.onResponse(null);
                    return;
                }

                double medianDifference = calculateMedianDifference(timestamps);

                long minimumMinutes = millisecondsToCeilMinutes(((Double) medianDifference).longValue());
                if (minimumMinutes > TimeSeriesSettings.MAX_INTERVAL_REC_LENGTH_IN_MINUTES) {
                    logger.warn("The minimum interval is too large: {}", minimumMinutes);
                    listener.onResponse(null);
                    return;
                }
                listener.onResponse(new IntervalTimeConfiguration(minimumMinutes, ChronoUnit.MINUTES));
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
        // make sure it is sorted
        Collections.sort(timestamps);
        List<Long> differences = new ArrayList<>();
        for (int i = 1; i < timestamps.size(); i++) {
            differences.add(timestamps.get(i) - timestamps.get(i - 1));
        }

        Collections.sort(differences);

        int middle = differences.size() / 2;
        if (differences.size() % 2 == 0) {
            // If even, choose the lower of the two middle values (same as numpy.median behavior for integers)
            return differences.get(middle - 1);
        } else {
            // If odd, return the middle value
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
}
