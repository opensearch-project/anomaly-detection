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

package org.opensearch.timeseries.feature;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.timeseries.annotation.Generated;

/**
 * Data object for features internally used with ML.
 */
public class Features {

    private final List<Entry<Long, Long>> timeRanges;
    private final double[][] unprocessedFeatures;

    /**
     * Constructor with all arguments.
     *
     * @param timeRanges the time ranges of feature data points. The time is of epoch milliseconds.
     * @param unprocessedFeatures unprocessed feature values (such as from aggregates from search)
     */
    public Features(List<Entry<Long, Long>> timeRanges, double[][] unprocessedFeatures) {
        this.timeRanges = timeRanges;
        this.unprocessedFeatures = unprocessedFeatures;
    }

    /**
     * Returns the time ranges of feature data points.
     *
     * @return list of pairs of start and end in epoch milliseconds
     */
    public List<Entry<Long, Long>> getTimeRanges() {
        return timeRanges;
    }

    /**
     * Returns unprocessed features (such as from aggregates from search).
     *
     * @return unprocessed features of data points
     */
    public double[][] getUnprocessedFeatures() {
        return unprocessedFeatures;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Features that = (Features) o;
        return Objects.equals(this.timeRanges, that.timeRanges) && Arrays.deepEquals(this.unprocessedFeatures, that.unprocessedFeatures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeRanges, unprocessedFeatures);
    }

    @Generated
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("timeRanges", timeRanges).append("unprocessedFeatures", unprocessedFeatures).toString();
    }
}
