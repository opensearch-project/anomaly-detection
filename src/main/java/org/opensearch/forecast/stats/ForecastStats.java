/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.stats;

import java.util.Map;

import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.stats.TimeSeriesStat;

public class ForecastStats extends Stats {

    public ForecastStats(Map<String, TimeSeriesStat<?>> stats) {
        super(stats);
    }

}
