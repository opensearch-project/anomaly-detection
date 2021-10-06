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

package org.opensearch.ad.ml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class RcfResultTests {

    private double score = 1.;
    private double confidence = 0;
    private int forestSize = 10;
    private int totalUpdate = 32;
    private double grade = 0.1;
    private double[] attribution = new double[] { 1. };
    private RcfResult rcfResult = new RcfResult(score, confidence, forestSize, attribution, totalUpdate, grade);

    @Test
    public void getters_returnExcepted() {
        assertEquals(score, rcfResult.getScore(), 1e-8);
        assertEquals(forestSize, rcfResult.getForestSize());
        assertTrue(Arrays.equals(attribution, rcfResult.getAttribution()));
    }

    private Object[] equalsData() {
        return new Object[] {
            new Object[] { rcfResult, null, false },
            new Object[] { rcfResult, rcfResult, true },
            new Object[] { rcfResult, 1, false },
            new Object[] { rcfResult, new RcfResult(score, confidence, forestSize, attribution, totalUpdate, grade), true },
            new Object[] { rcfResult, new RcfResult(score + 1, confidence, forestSize, attribution, totalUpdate, grade), false },
            new Object[] { rcfResult, new RcfResult(score, confidence, forestSize + 1, attribution, totalUpdate, grade), false },
            new Object[] { rcfResult, new RcfResult(score + 1, confidence, forestSize + 1, attribution, totalUpdate, grade), false },
            new Object[] { rcfResult, new RcfResult(score, confidence + 1, forestSize, attribution, totalUpdate, grade), false },
            new Object[] { rcfResult, new RcfResult(score, confidence, forestSize, new double[] { 2. }, totalUpdate, grade), false }, };
    }

    @Test
    @Parameters(method = "equalsData")
    public void equals_returnExpected(RcfResult result, Object other, boolean expected) {
        assertEquals(expected, result.equals(other));
    }

    private Object[] hashCodeData() {
        return new Object[] {
            new Object[] { rcfResult, new RcfResult(score, confidence, forestSize, attribution, totalUpdate, grade), true },
            new Object[] { rcfResult, new RcfResult(score + 1, confidence, forestSize, attribution, totalUpdate, grade), false },
            new Object[] { rcfResult, new RcfResult(score, confidence, forestSize + 1, attribution, totalUpdate, grade), false },
            new Object[] { rcfResult, new RcfResult(score + 1, confidence, forestSize + 1, attribution, totalUpdate, grade), false },
            new Object[] { rcfResult, new RcfResult(score, confidence, forestSize, new double[] { 2. }, totalUpdate, grade), false }, };
    }

    @Test
    @Parameters(method = "hashCodeData")
    public void hashCode_returnExpected(RcfResult result, RcfResult other, boolean expected) {
        assertEquals(expected, result.hashCode() == other.hashCode());
    }
}
