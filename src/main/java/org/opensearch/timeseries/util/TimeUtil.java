/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;

public class TimeUtil {
    public static long calculateTimeoutMillis(Config config, long dataEndTimeMillis) {
        long windowDelayMillis = config.getWindowDelay() == null
            ? 0
            : ((IntervalTimeConfiguration) config.getWindowDelay()).toDuration().toMillis();
        long nextExecutionEnd = dataEndTimeMillis + config.getIntervalInMilliseconds() + windowDelayMillis;
        return nextExecutionEnd;
    }
}
