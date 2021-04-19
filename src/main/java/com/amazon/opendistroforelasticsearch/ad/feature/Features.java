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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.feature;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Data object for features internally used with ML.
 */
public class Features {

    private final List<Entry<Long, Long>> timeRanges;
    private final double[][] unprocessedFeatures;
    private final double[][] processedFeatures;

    /**
     * Constructor with all arguments.
     *
     * @param timeRanges the time ranges of feature data points
     * @param unprocessedFeatures unprocessed feature values (such as from aggregates from search)
     * @param processedFeatures processed feature values (such as shingle)
     */
    public Features(List<Entry<Long, Long>> timeRanges, double[][] unprocessedFeatures, double[][] processedFeatures) {
        this.timeRanges = timeRanges;
        this.unprocessedFeatures = unprocessedFeatures;
        this.processedFeatures = processedFeatures;
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

    /**
     * Returns processed features (such as shingle).
     *
     * @return processed features of data points
     */
    public double[][] getProcessedFeatures() {
        return processedFeatures;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Features that = (Features) o;
        return Objects.equals(this.timeRanges, that.timeRanges)
            && Arrays.deepEquals(this.unprocessedFeatures, that.unprocessedFeatures)
            && Arrays.deepEquals(this.processedFeatures, that.processedFeatures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeRanges, unprocessedFeatures, processedFeatures);
    }
}
