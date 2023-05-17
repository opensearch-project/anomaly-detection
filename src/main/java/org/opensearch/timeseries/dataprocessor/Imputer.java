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

/*
 * An object for imputing feature vectors.
 *
 * In certain situations, due to time and compute cost, we are only allowed to
 * query a sparse sample of data points / feature vectors from a cluster.
 * However, we need a large sample of feature vectors in order to train our
 * anomaly detection algorithms. An Imputer approximates the data points
 * between a given, ordered list of samples.
 */
public abstract class Imputer {

    /**
     * Imputes the given sample feature vectors.
     *
     * Computes a list `numImputed` feature vectors using the ordered list
     * of `numSamples` input sample vectors where each sample vector has size
     * `numFeatures`.
     *
     *
     * @param samples          A `numFeatures x numSamples` list of feature vectors.
     * @param numImputed  The desired number of imputed vectors.
     * @return                 A `numFeatures x numImputed` list of feature vectors.
     */
    public double[][] impute(double[][] samples, int numImputed) {
        int numFeatures = samples.length;
        double[][] interpolants = new double[numFeatures][numImputed];

        for (int featureIndex = 0; featureIndex < numFeatures; featureIndex++) {
            interpolants[featureIndex] = singleFeatureImpute(samples[featureIndex], numImputed);
        }
        return interpolants;
    }

    /**
     * compute per-feature impute value
     * @param samples input array
     * @param numImputed number of elements in the return array
     * @return input array with missing values imputed
     */
    protected abstract double[] singleFeatureImpute(double[] samples, int numImputed);
}
