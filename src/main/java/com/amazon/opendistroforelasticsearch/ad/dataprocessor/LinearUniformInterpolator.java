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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.dataprocessor;

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
