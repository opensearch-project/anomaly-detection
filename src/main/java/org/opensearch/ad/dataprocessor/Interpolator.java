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

package org.opensearch.ad.dataprocessor;

/*
 * An object for interpolating feature vectors.
 *
 * In certain situations, due to time and compute cost, we are only allowed to
 * query a sparse sample of data points / feature vectors from a cluster.
 * However, we need a large sample of feature vectors in order to train our
 * anomaly detection algorithms. An Interpolator approximates the data points
 * between a given, ordered list of samples.
 */
public interface Interpolator {

    /*
     * Interpolates the given sample feature vectors.
     *
     * Computes a list `numInterpolants` feature vectors using the ordered list
     * of `numSamples` input sample vectors where each sample vector has size
     * `numFeatures`.
     *
     * @param samples          A `numFeatures x numSamples` list of feature vectors.
     * @param numInterpolants  The desired number of interpolating vectors.
     * @return                 A `numFeatures x numInterpolants` list of feature vectors.
     */
    double[][] interpolate(double[][] samples, int numInterpolants);
}
