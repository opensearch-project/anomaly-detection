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

import java.util.Arrays;

/*
 * A piecewise linear interpolator in a single feature dimension.
 *
 * A utility class for LinearUniformInterpolator. Constructs uniformly spaced
 * piecewise linear interpolations within a single feature dimension.
 *
 * @see LinearUniformInterpolator
 */
public class SingleFeatureLinearUniformInterpolator {

    /*
     * Piecewise linearly interpolates the given sample of one-dimensional
     * features.
     *
     * Computes a list `numInterpolants` features using the ordered list of
     * `numSamples` input one-dimensional samples. The interpolant features are
     * computing using a piecewise linear interpolation.
     *
     * @param samples         A `numSamples` sized list of sample features.
     * @param numInterpolants The desired number of interpolating features.
     * @return                A `numInterpolants` sized array of interpolant features.
     * @see LinearUniformInterpolator
     */
    public double[] interpolate(double[] samples, int numInterpolants) {
        int numSamples = samples.length;
        double[] interpolants = new double[numInterpolants];

        if (numSamples == 0) {
            interpolants = new double[0];
        } else if (numSamples == 1) {
            Arrays.fill(interpolants, samples[0]);
        } else {
            /* assume the piecewise linear interpolation between the samples is a
             parameterized curve f(t) for t in [0, 1]. Each pair of samples
             determines a interval [t_i, t_(i+1)]. For each interpolant we determine
             which interval it lies inside and then scale the value of t,
             accordingly to compute the interpolant value.
            
             for numerical stability reasons we omit processing the final
             interpolant in this loop since this last interpolant is always equal
             to the last sample.
            */
            for (int interpolantIndex = 0; interpolantIndex < (numInterpolants - 1); interpolantIndex++) {
                double tGlobal = ((double) interpolantIndex) / (numInterpolants - 1.0);
                double tInterval = tGlobal * (numSamples - 1.0);
                int intervalIndex = (int) Math.floor(tInterval);
                tInterval -= intervalIndex;

                double leftSample = samples[intervalIndex];
                double rightSample = samples[intervalIndex + 1];
                double interpolant = (1.0 - tInterval) * leftSample + tInterval * rightSample;
                interpolants[interpolantIndex] = interpolant;
            }

            // the final interpolant is always the final sample
            interpolants[numInterpolants - 1] = samples[numSamples - 1];
        }
        return interpolants;
    }
}
