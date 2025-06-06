/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;

import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.SuggestConfigParamResponse;

public class HistorySuggest {
    private Config config;
    private User user;
    private SearchFeatureDao searchFeatureDao;
    private IntervalTimeConfiguration intervalRecommended;
    private Map<String, Object> topEntity;
    private Clock clock;

    public HistorySuggest(
        Config config,
        User user,
        SearchFeatureDao searchFeatureDao,
        IntervalTimeConfiguration intervalRecommended,
        Map<String, Object> topEntity,
        Clock clock
    ) {
        this.config = config;
        this.user = user;
        this.searchFeatureDao = searchFeatureDao;
        this.intervalRecommended = intervalRecommended;
        this.topEntity = topEntity;
        this.clock = clock;
    }

    public void suggestHistory(ActionListener<SuggestConfigParamResponse> listener) {
        if (intervalRecommended == null) {
            listener.onResponse(new SuggestConfigParamResponse.Builder().history(config.getDefaultHistory()).build());
            return;
        }
        searchFeatureDao.getDateRangeOfSourceData(config, user, topEntity, ActionListener.wrap(minMax -> {
            long minEpoch = minMax.getLeft();
            long maxEpoch = minMax.getRight();
            // in case of future date exists, use current time as maxEpoch
            if (maxEpoch > clock.millis()) {
                maxEpoch = clock.millis();
            }
            int history = getNumberOfIntervals(minEpoch, maxEpoch, intervalRecommended);
            listener
                .onResponse(
                    new SuggestConfigParamResponse.Builder()
                        .history(Math.max(config.getDefaultHistory(), Math.min(history, TimeSeriesSettings.MAX_HISTORY_INTERVALS)))
                        .build()
                );
        }, listener::onFailure));
    }

    /**
     * Calculates how many full intervals fit between startEpochMillis (inclusive)
     * and endEpochMillis (exclusive).
     *
     * @param startEpochMillis The start time in epoch milliseconds
     * @param endEpochMillis   The end time in epoch milliseconds
     * @param intervalConfig   The IntervalTimeConfiguration defining the length of each interval
     * @return The number of full intervals between start and end
     */
    private int getNumberOfIntervals(long startEpochMillis, long endEpochMillis, IntervalTimeConfiguration intervalConfig) {
        if (endEpochMillis <= startEpochMillis) {
            return 0;  // No intervals if the range is zero or negative
        }

        // Convert the configured interval to a Duration, then get the length in ms
        Duration intervalDuration = intervalConfig.toDuration();
        long intervalMillis = intervalDuration.toMillis();
        if (intervalMillis <= 0) {
            throw new IllegalArgumentException("Interval duration must be positive");
        }

        // Compute total milliseconds in [start, end) and divide by the interval length
        long totalRangeMillis = endEpochMillis - startEpochMillis;
        return (int) (totalRangeMillis / intervalMillis);
    }

}
