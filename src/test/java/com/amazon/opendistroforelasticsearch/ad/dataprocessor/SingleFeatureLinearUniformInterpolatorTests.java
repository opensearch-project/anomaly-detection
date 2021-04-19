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

package com.amazon.opendistroforelasticsearch.ad.dataprocessor;

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SingleFeatureLinearUniformInterpolatorTests {

    @Parameters
    public static Collection<Object[]> data() {
        double[] singleComponent = { -1.0, 2.0 };
        double[] multiComponent = { 0.0, 1.0, -1. };
        double oneThird = 1.0 / 3.0;

        return Arrays
            .asList(
                new Object[][] {
                    { new double[0], 1, new double[0] },
                    { new double[] { 1 }, 2, new double[] { 1, 1 } },
                    { singleComponent, 2, singleComponent },
                    { singleComponent, 3, new double[] { -1.0, 0.5, 2.0 } },
                    { singleComponent, 4, new double[] { -1.0, 0.0, 1.0, 2.0 } },
                    { multiComponent, 3, multiComponent },
                    { multiComponent, 4, new double[] { 0.0, 2 * oneThird, oneThird, -1.0 } },
                    { multiComponent, 5, new double[] { 0.0, 0.5, 1.0, 0.0, -1.0 } },
                    { multiComponent, 6, new double[] { 0.0, 0.4, 0.8, 0.6, -0.2, -1.0 } } }
            );
    }

    private double[] input;
    private int numInterpolants;
    private double[] expected;
    private SingleFeatureLinearUniformInterpolator interpolator;

    public SingleFeatureLinearUniformInterpolatorTests(double[] input, int numInterpolants, double[] expected) {
        this.input = input;
        this.numInterpolants = numInterpolants;
        this.expected = expected;
    }

    @Before
    public void setUp() {
        this.interpolator = new SingleFeatureLinearUniformInterpolator();
    }

    @Test
    public void testInterpolation() {
        double[] actual = interpolator.interpolate(input, numInterpolants);
        double delta = 1e-8;
        assertArrayEquals(expected, actual, delta);
    }
}
