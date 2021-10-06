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
