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

import com.google.common.math.DoubleMath;

/**
 * A piecewise linear imputer with uniformly spaced points.
 *
 * The LinearUniformImputer constructs a piecewise linear imputation on
 * the input list of sample feature vectors. That is, between every consecutive
 * pair of points we construct a linear imputation. The linear imputation
 * is computed on a per-feature basis.
 *
 */
public class LinearUniformImputer extends Imputer {
    // if true, impute integral/floating-point results: when all samples are integral,
    // the results are integral. Else, the results are floating points.
    private boolean integerSensitive;

    public LinearUniformImputer(boolean integerSensitive) {
        this.integerSensitive = integerSensitive;
    }

    /*
     * Piecewise linearly impute the given sample of one-dimensional
     * features.
     *
     * Computes a list `numImputed` features using the ordered list of
     * `numSamples` input one-dimensional samples. The imputed features are
     * computing using a piecewise linear imputation.
     *
     * @param samples         A `numSamples` sized list of sample features.
     * @param numImputed      The desired number of imputed features.
     * @return                A `numImputed` sized array of imputed features.
     */
    @Override
    public double[] singleFeatureImpute(double[] samples, int numImputed) {
        int numSamples = samples.length;
        double[] imputedValues = new double[numImputed];

        if (numSamples == 0) {
            imputedValues = new double[0];
        } else if (numSamples == 1) {
            Arrays.fill(imputedValues, samples[0]);
        } else {
            /* assume the piecewise linear imputation between the samples is a
             parameterized curve f(t) for t in [0, 1]. Each pair of samples
             determines a interval [t_i, t_(i+1)]. For each imputed value we
             determine which interval it lies inside and then scale the value of t,
             accordingly to compute the imputed value.
            
             for numerical stability reasons we omit processing the final
             imputed value in this loop since this last imputed value is always equal
             to the last sample.
            */
            for (int imputedIndex = 0; imputedIndex < (numImputed - 1); imputedIndex++) {
                double tGlobal = (imputedIndex) / (numImputed - 1.0);
                double tInterval = tGlobal * (numSamples - 1.0);
                int intervalIndex = (int) Math.floor(tInterval);
                tInterval -= intervalIndex;

                double leftSample = samples[intervalIndex];
                double rightSample = samples[intervalIndex + 1];
                double imputed = (1.0 - tInterval) * leftSample + tInterval * rightSample;
                imputedValues[imputedIndex] = imputed;
            }

            // the final imputed value is always the final sample
            imputedValues[numImputed - 1] = samples[numSamples - 1];
        }
        if (integerSensitive && Arrays.stream(samples).allMatch(DoubleMath::isMathematicalInteger)) {
            imputedValues = Arrays.stream(imputedValues).map(Math::rint).toArray();
        }
        return imputedValues;
    }
}
