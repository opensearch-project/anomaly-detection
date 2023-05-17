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

import java.util.Arrays;

/**
 * fixing missing value (denoted using Double.NaN) using 0.
 * The 2nd parameter of impute is ignored as we infer the number
 * of imputed values using the number of Double.NaN.
 */
public class ZeroImputer extends Imputer {

    @Override
    public double[] singleFeatureImpute(double[] samples, int numInterpolants) {
        return Arrays.stream(samples).map(d -> Double.isNaN(d) ? 0.0 : d).toArray();
    }
}
