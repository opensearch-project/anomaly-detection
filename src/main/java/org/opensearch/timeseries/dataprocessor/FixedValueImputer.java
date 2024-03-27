/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.dataprocessor;

import java.util.Arrays;

/**
 * fixing missing value (denoted using Double.NaN) using a fixed set of specified values.
 * The 2nd parameter of interpolate is ignored as we infer the number of imputed values
 * using the number of Double.NaN.
 */
public class FixedValueImputer extends Imputer {
    private double[] fixedValue;

    public FixedValueImputer(double[] fixedValue) {
        this.fixedValue = fixedValue;
    }

    /**
     * Given an array of samples, fill with given value.
     * We will ignore the rest of samples beyond the 2nd element.
     *
     * @return an imputed array of size numImputed
     */
    @Override
    public double[][] impute(double[][] samples, int numImputed) {
        int numFeatures = samples.length;
        double[][] imputed = new double[numFeatures][numImputed];

        for (int featureIndex = 0; featureIndex < numFeatures; featureIndex++) {
            imputed[featureIndex] = singleFeatureInterpolate(samples[featureIndex], numImputed, fixedValue[featureIndex]);
        }
        return imputed;
    }

    private double[] singleFeatureInterpolate(double[] samples, int numInterpolants, double defaultVal) {
        return Arrays.stream(samples).map(d -> Double.isNaN(d) ? defaultVal : d).toArray();
    }

    @Override
    protected double[] singleFeatureImpute(double[] samples, int numInterpolants) {
        throw new UnsupportedOperationException("The operation is not supported");
    }
}
