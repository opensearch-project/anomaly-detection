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

package org.opensearch.ad.model;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Set;

import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.annotation.Generated;
import org.opensearch.timeseries.constant.CommonMessages;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

public class IntervalTimeConfiguration extends TimeConfiguration {

    private long interval;
    private ChronoUnit unit;

    private static final Set<ChronoUnit> SUPPORTED_UNITS = ImmutableSet.of(ChronoUnit.MINUTES, ChronoUnit.SECONDS);

    /**
     * Constructor function.
     *
     * @param interval interval period value
     * @param unit     time unit
     */
    public IntervalTimeConfiguration(long interval, ChronoUnit unit) {
        if (interval < 0) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Interval %s %s", interval, CommonMessages.NEGATIVE_TIME_CONFIGURATION)
            );
        }
        if (!SUPPORTED_UNITS.contains(unit)) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, ADCommonMessages.INVALID_TIME_CONFIGURATION_UNITS, unit));
        }
        this.interval = interval;
        this.unit = unit;
    }

    public IntervalTimeConfiguration(StreamInput input) throws IOException {
        this.interval = input.readLong();
        this.unit = input.readEnum(ChronoUnit.class);
    }

    public static IntervalTimeConfiguration readFrom(StreamInput input) throws IOException {
        return new IntervalTimeConfiguration(input);
    }

    public static long getIntervalInMinute(IntervalTimeConfiguration interval) {
        if (interval.getUnit() == ChronoUnit.SECONDS) {
            return interval.getInterval() / 60;
        }
        return interval.getInterval();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(this.interval);
        out.writeEnum(this.unit);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().startObject(PERIOD_FIELD).field(INTERVAL_FIELD, interval).field(UNIT_FIELD, unit).endObject().endObject();
        return builder;
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        IntervalTimeConfiguration that = (IntervalTimeConfiguration) o;
        return getInterval() == that.getInterval() && getUnit() == that.getUnit();
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(interval, unit);
    }

    public long getInterval() {
        return interval;
    }

    public ChronoUnit getUnit() {
        return unit;
    }

    /**
     * Returns the duration of the interval.
     *
     * @return the duration of the interval
     */
    public Duration toDuration() {
        return Duration.of(interval, unit);
    }
}
