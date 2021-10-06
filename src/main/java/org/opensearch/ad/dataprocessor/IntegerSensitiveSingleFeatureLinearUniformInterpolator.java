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
