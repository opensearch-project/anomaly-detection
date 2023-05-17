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

import org.junit.Test;

public class FixedValueImputerTests {

    @Test
    public void testImpute() {
        // Initialize the FixedValueImputer with some fixed values
        double[] fixedValues = { 2.0, 3.0 };
        FixedValueImputer imputer = new FixedValueImputer(fixedValues);

        // Create a sample array with some missing values (Double.NaN)
        double[][] samples = { { 1.0, Double.NaN, 3.0 }, { Double.NaN, 2.0, 3.0 } };

        // Call the impute method
        double[][] imputed = imputer.impute(samples, 3);

        // Check the results
        double[][] expected = { { 1.0, 2.0, 3.0 }, { 3.0, 2.0, 3.0 } };
        double delta = 0.0001;

        for (int i = 0; i < expected.length; i++) {
            assertArrayEquals("The arrays are not equal", expected[i], imputed[i], delta);
        }
    }
}
