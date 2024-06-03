/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.timeseries.util;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;

import org.opensearch.common.unit.TimeValue;

public class DateUtils {
    public static final ZoneId UTC = ZoneId.of("Z");

    /**
     * Get hour of day of the input time in UTC
     * @param instant input time
     *
     * @return Hour of day
     */
    public static int getUTCHourOfDay(Instant instant) {
        ZonedDateTime time = ZonedDateTime.ofInstant(instant, UTC);
        return time.get(ChronoField.HOUR_OF_DAY);
    }

    public static Duration toDuration(TimeValue timeValue) {
        return Duration.ofMillis(timeValue.millis());
    }
}
