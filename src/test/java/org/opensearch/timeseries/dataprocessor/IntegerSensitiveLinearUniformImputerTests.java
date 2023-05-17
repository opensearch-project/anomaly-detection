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

package org.opensearch.timeseries.dataprocessor;

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Compared to MultiFeatureLinearUniformImputerTests, outputs are different and
 * integerSensitive is enabled
 *
 */
@RunWith(Parameterized.class)
public class IntegerSensitiveLinearUniformImputerTests {

    @Parameters
    public static Collection<Object[]> data() {
        double[][] singleComponent = { { -1.0, 2.0 }, { 1.0, 1.0 } };
        double[][] multiComponent = { { 0.0, 1.0, -1.0 }, { 1.0, 1.0, 1.0 } };

        return Arrays
            .asList(
                new Object[][] {
                    // after integer sensitive rint rounding
                    { singleComponent, 2, singleComponent },
                    { singleComponent, 3, new double[][] { { -1.0, 0, 2.0 }, { 1.0, 1.0, 1.0 } } },
                    { singleComponent, 4, new double[][] { { -1.0, 0.0, 1.0, 2.0 }, { 1.0, 1.0, 1.0, 1.0 } } },
                    { multiComponent, 3, multiComponent },
                    { multiComponent, 4, new double[][] { { 0.0, 1.0, 0.0, -1.0 }, { 1.0, 1.0, 1.0, 1.0 } } },
                    { multiComponent, 5, new double[][] { { 0.0, 0.0, 1.0, 0.0, -1.0 }, { 1.0, 1.0, 1.0, 1.0, 1.0 } } },
                    { multiComponent, 6, new double[][] { { 0.0, 0.0, 1.0, 1.0, -0.0, -1.0 }, { 1.0, 1.0, 1.0, 1.0, 1.0, 1.0 } } }, }
            );
    }

    private double[][] input;
    private int numInterpolants;
    private double[][] expected;
    private Imputer imputer;

    public IntegerSensitiveLinearUniformImputerTests(double[][] input, int numInterpolants, double[][] expected) {
        this.input = input;
        this.numInterpolants = numInterpolants;
        this.expected = expected;
    }

    @Before
    public void setUp() {
        this.imputer = new LinearUniformImputer(true);
    }

    @Test
    public void testImputation() {
        double[][] actual = imputer.impute(input, numInterpolants);
        double delta = 1e-8;
        int numFeatures = expected.length;

        for (int i = 0; i < numFeatures; i++) {
            assertArrayEquals(expected[i], actual[i], delta);
        }
    }
}
