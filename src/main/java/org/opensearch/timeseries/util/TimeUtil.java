/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import java.time.Duration;

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

    public static boolean isMultiple(Duration a, Duration b) {
        if (b.isZero())
            return a.isZero(); // define 0 as a multiple of 0; change to 'false' if you prefer
        long aNanos = a.toNanos();
        long bNanos = b.toNanos();
        return bNanos != 0 && aNanos % bNanos == 0;
    }

    public static long getMultiple(Duration a, Duration b) {
        if (b.isZero())
            return 0;
        long aNanos = a.toNanos();
        long bNanos = b.toNanos();
        return aNanos / bNanos;
    }
}
