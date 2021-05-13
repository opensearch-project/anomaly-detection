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

import java.util.Arrays;

import com.google.common.math.DoubleMath;

/**
 * Interpolator sensitive to integral values.
 */
public class IntegerSensitiveSingleFeatureLinearUniformInterpolator extends SingleFeatureLinearUniformInterpolator {

    /**
     * Interpolates integral/floating-point results.
     *
     * If all samples are integral, the results are integral.
     * Else, the results are floating points.
     *
     * @param samples integral/floating-point samples
     * @param numInterpolants the number of interpolants
     * @return {code numInterpolants} interpolated results
     */
    public double[] interpolate(double[] samples, int numInterpolants) {
        double[] interpolants = super.interpolate(samples, numInterpolants);
        if (Arrays.stream(samples).allMatch(DoubleMath::isMathematicalInteger)) {
            interpolants = Arrays.stream(interpolants).map(Math::rint).toArray();
        }
        return interpolants;
    }
}
