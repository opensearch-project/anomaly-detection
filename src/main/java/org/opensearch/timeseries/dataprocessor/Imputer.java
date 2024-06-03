/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.dataprocessor;

import static org.apache.commons.math3.linear.MatrixUtils.createRealMatrix;

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
     * @param samples          A `numSamples x numFeatures` list of feature vectors.
     * @param numImputed  The desired number of imputed vectors.
     * @return                 A `numImputed x numFeatures` list of feature vectors.
     */
    public double[][] impute(double[][] samples, int numImputed) {
        // convert to a `numFeatures x numSamples` list of feature vectors
        double[][] transposed = transpose(samples);
        int numFeatures = transposed.length;
        double[][] imputants = new double[numFeatures][numImputed];
        for (int featureIndex = 0; featureIndex < numFeatures; featureIndex++) {
            imputants[featureIndex] = singleFeatureImpute(transposed[featureIndex], numImputed);
        }
        // transpose back to a `numSamples x numFeatures` list of feature vectors
        return transpose(imputants);
    }

    /**
     * compute per-feature impute value
     * @param samples input array
     * @param numImputed number of elements in the return array
     * @return input array with missing values imputed
     */
    public abstract double[] singleFeatureImpute(double[] samples, int numImputed);

    private double[][] transpose(double[][] matrix) {
        return createRealMatrix(matrix).transpose().getData();
    }
}
