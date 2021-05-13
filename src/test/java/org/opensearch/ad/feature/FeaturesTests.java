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

package org.opensearch.ad.feature;

import static org.junit.Assert.assertEquals;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class FeaturesTests {

    private List<Entry<Long, Long>> ranges = Arrays.asList(new SimpleEntry<>(0L, 1L));
    private double[][] unprocessed = new double[][] { { 1, 2 } };
    private double[][] processed = new double[][] { { 3, 4 } };

    private Features features = new Features(ranges, unprocessed, processed);

    @Test
    public void getters_returnExcepted() {
        assertEquals(ranges, features.getTimeRanges());
        assertEquals(unprocessed, features.getUnprocessedFeatures());
        assertEquals(processed, features.getProcessedFeatures());
    }

    private Object[] equalsData() {
        return new Object[] {
            new Object[] { features, features, true },
            new Object[] { features, new Features(ranges, unprocessed, processed), true },
            new Object[] { features, null, false },
            new Object[] { features, "testString", false },
            new Object[] { features, new Features(null, unprocessed, processed), false },
            new Object[] { features, new Features(ranges, null, processed), false },
            new Object[] { features, new Features(ranges, unprocessed, null), false }, };
    }

    @Test
    @Parameters(method = "equalsData")
    public void equals_returnExpected(Features result, Object other, boolean expected) {
        assertEquals(expected, result.equals(other));
    }

    private Object[] hashCodeData() {
        Features features = new Features(ranges, unprocessed, processed);
        return new Object[] {
            new Object[] { features, new Features(ranges, unprocessed, processed), true },
            new Object[] { features, new Features(null, unprocessed, processed), false },
            new Object[] { features, new Features(ranges, null, processed), false },
            new Object[] { features, new Features(ranges, unprocessed, null), false }, };
    }

    @Test
    @Parameters(method = "hashCodeData")
    public void hashCode_returnExpected(Features result, Features other, boolean expected) {
        assertEquals(expected, result.hashCode() == other.hashCode());
    }
}
