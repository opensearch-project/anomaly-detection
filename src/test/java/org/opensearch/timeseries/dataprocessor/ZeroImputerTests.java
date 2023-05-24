/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.dataprocessor;

import static org.junit.Assert.assertArrayEquals;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class ZeroImputerTests {

    private Imputer imputer;

    @Before
    public void setup() {
        imputer = new ZeroImputer();
    }

    private Object[] imputeData() {
        return new Object[] {
            new Object[] { new double[] { 25.25, Double.NaN, 25.75 }, 3, new double[] { 25.25, 0, 25.75 } },
            new Object[] { new double[] { Double.NaN, 25, 75 }, 3, new double[] { 0, 25, 75 } },
            new Object[] { new double[] { 25, 75.5, Double.NaN }, 3, new double[] { 25, 75.5, 0 } }, };
    }

    @Test
    @Parameters(method = "imputeData")
    public void impute_returnExpected(double[] samples, int num, double[] expected) {
        assertArrayEquals("The arrays are not equal", expected, imputer.singleFeatureImpute(samples, num), 0.001);
    }
}
