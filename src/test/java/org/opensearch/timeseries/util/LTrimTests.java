/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import org.opensearch.test.OpenSearchTestCase;

public class LTrimTests extends OpenSearchTestCase {

    public void testLtrimEmptyArray() {

        double[][] input = {};
        double[][] expectedOutput = {};

        assertArrayEquals(expectedOutput, DataUtil.ltrim(input));
    }

    public void testLtrimAllNaN() {

        double[][] input = { { Double.NaN, Double.NaN }, { Double.NaN, Double.NaN }, { Double.NaN, Double.NaN } };
        double[][] expectedOutput = {};

        assertArrayEquals(expectedOutput, DataUtil.ltrim(input));
    }

    public void testLtrimSomeNaN() {

        double[][] input = { { Double.NaN, Double.NaN }, { 1.0, 2.0 }, { 3.0, 4.0 } };
        double[][] expectedOutput = { { 1.0, 2.0 }, { 3.0, 4.0 } };

        assertArrayEquals(expectedOutput, DataUtil.ltrim(input));
    }

    public void testLtrimNoNaN() {

        double[][] input = { { 1.0, 2.0 }, { 3.0, 4.0 }, { 5.0, 6.0 } };
        double[][] expectedOutput = { { 1.0, 2.0 }, { 3.0, 4.0 }, { 5.0, 6.0 } };

        assertArrayEquals(expectedOutput, DataUtil.ltrim(input));
    }
}
