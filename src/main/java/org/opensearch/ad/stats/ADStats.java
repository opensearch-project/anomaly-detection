/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.stats;

import java.util.Map;

import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.stats.TimeSeriesStat;

public class ADStats extends Stats {

    public ADStats(Map<String, TimeSeriesStat<?>> stats) {
        super(stats);
    }

}
