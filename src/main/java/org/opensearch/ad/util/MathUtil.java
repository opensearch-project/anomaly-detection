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

package org.opensearch.ad.util;

public class MathUtil {
    /*
     * Private constructor to avoid Jacoco complain about public constructor
     * not covered: https://tinyurl.com/yetc7tra
     */
    private MathUtil() {}

    /**
     * Function to calculate the log base 2 of an integer
     *
     * @param N input number
     * @return the base 2 logarithm of an integer value
     */
    public static double log2(int N) {
        // calculate log2 N indirectly using log() method
        return Math.log(N) / Math.log(2);
    }

}
