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

package org.opensearch.ad.feature;

import static org.junit.Assert.assertEquals;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.opensearch.timeseries.feature.Features;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class FeaturesTests {

    private List<Entry<Long, Long>> ranges = Arrays.asList(new SimpleEntry<>(0L, 1L));
    private double[][] unprocessed = new double[][] { { 1, 2 } };

    private Features features = new Features(ranges, unprocessed);

    @Test
    public void getters_returnExcepted() {
        assertEquals(ranges, features.getTimeRanges());
        assertEquals(unprocessed, features.getUnprocessedFeatures());
    }

    private Object[] equalsData() {
        return new Object[] {
            new Object[] { features, features, true },
            new Object[] { features, new Features(ranges, unprocessed), true },
            new Object[] { features, null, false },
            new Object[] { features, "testString", false },
            new Object[] { features, new Features(null, unprocessed), false },
            new Object[] { features, new Features(ranges, null), false }, };
    }

    @Test
    @Parameters(method = "equalsData")
    public void equals_returnExpected(Features result, Object other, boolean expected) {
        assertEquals(expected, result.equals(other));
    }

    private Object[] hashCodeData() {
        Features features = new Features(ranges, unprocessed);
        return new Object[] {
            new Object[] { features, new Features(ranges, unprocessed), true },
            new Object[] { features, new Features(null, unprocessed), false },
            new Object[] { features, new Features(ranges, null), false }, };
    }

    @Test
    @Parameters(method = "hashCodeData")
    public void hashCode_returnExpected(Features result, Features other, boolean expected) {
        assertEquals(expected, result.hashCode() == other.hashCode());
    }
}
