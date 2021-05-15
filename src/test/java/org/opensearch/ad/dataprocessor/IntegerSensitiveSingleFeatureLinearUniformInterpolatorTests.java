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

package org.opensearch.ad.dataprocessor;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class IntegerSensitiveSingleFeatureLinearUniformInterpolatorTests {

    private IntegerSensitiveSingleFeatureLinearUniformInterpolator interpolator;

    @Before
    public void setup() {
        interpolator = new IntegerSensitiveSingleFeatureLinearUniformInterpolator();
    }

    private Object[] interpolateData() {
        return new Object[] {
            new Object[] { new double[] { 25.25, 25.75 }, 3, new double[] { 25.25, 25.5, 25.75 } },
            new Object[] { new double[] { 25, 75 }, 3, new double[] { 25, 50, 75 } },
            new Object[] { new double[] { 25, 75.5 }, 3, new double[] { 25, 50.25, 75.5 } }, };
    }

    @Test
    @Parameters(method = "interpolateData")
    public void interpolate_returnExpected(double[] samples, int num, double[] expected) {
        assertTrue(Arrays.equals(expected, interpolator.interpolate(samples, num)));
    }
}
