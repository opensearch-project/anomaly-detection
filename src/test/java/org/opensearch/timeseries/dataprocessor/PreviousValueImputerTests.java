/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.dataprocessor;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class PreviousValueImputerTests {
    @Test
    void testSingleFeatureImpute() {
        PreviousValueImputer imputer = new PreviousValueImputer();

        double[] samples = { 1.0, Double.NaN, 3.0, Double.NaN, 5.0 };
        double[] expected = { 1.0, 1.0, 3.0, 3.0, 5.0 };

        assertArrayEquals(expected, imputer.singleFeatureImpute(samples, 0), "Imputation failed");

        // The second test checks whether the method removes leading Double.NaN values from the array
        samples = new double[] { Double.NaN, 2.0, Double.NaN, 4.0 };
        expected = new double[] { Double.NaN, 2.0, 2.0, 4.0 };

        assertArrayEquals(expected, imputer.singleFeatureImpute(samples, 0), "Imputation failed with leading NaN");
    }
}
