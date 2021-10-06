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

/*
 * A piecewise linear interpolator with uniformly spaced points.
 *
 * The LinearUniformInterpolator constructs a piecewise linear interpolation on
 * the input list of sample feature vectors. That is, between every consecutive
 * pair of points we construct a linear interpolation. The linear interpolation
 * is computed on a per-feature basis.
 *
 * This class uses the helper class SingleFeatureLinearUniformInterpolator to
 * compute per-feature interpolants.
 *
 * @see SingleFeatureLinearUniformInterpolator
 */
public class LinearUniformInterpolator implements Interpolator {

    private SingleFeatureLinearUniformInterpolator singleFeatureLinearUniformInterpolator;

    public LinearUniformInterpolator(SingleFeatureLinearUniformInterpolator singleFeatureLinearUniformInterpolator) {
        this.singleFeatureLinearUniformInterpolator = singleFeatureLinearUniformInterpolator;
    }

    /*
     * Piecewise linearly interpolates the given sample feature vectors.
     *
     * Computes a list `numInterpolants` feature vectors using the ordered list
     * of `numSamples` input sample vectors where each sample vector has size
     * `numFeatures`. The feature vectors are computing using a piecewise linear
     * interpolation.
     *
     * @param samples         A `numFeatures x numSamples` list of feature vectors.
     * @param numInterpolants The desired number of interpolating feature vectors.
     * @return                A `numFeatures x numInterpolants` list of feature vectors.
     * @see SingleFeatureLinearUniformInterpolator
     */
    public double[][] interpolate(double[][] samples, int numInterpolants) {
        int numFeatures = samples.length;
        double[][] interpolants = new double[numFeatures][numInterpolants];

        for (int featureIndex = 0; featureIndex < numFeatures; featureIndex++) {
            interpolants[featureIndex] = this.singleFeatureLinearUniformInterpolator.interpolate(samples[featureIndex], numInterpolants);
        }
        return interpolants;
    }
}
