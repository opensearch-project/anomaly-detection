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

package org.opensearch.ad.util;

public class MultiDimDataWithTime {
    public double[][] data;
    public long[] changeTimeStampsMs;
    public double[][] changes;
    public long[] timestampsMs;

    public MultiDimDataWithTime(double[][] data, long[] changeTimestamps, double[][] changes, long[] timestampsMs) {
        this.data = data;
        this.changeTimeStampsMs = changeTimestamps;
        this.changes = changes;
        this.timestampsMs = timestampsMs;
    }
}
