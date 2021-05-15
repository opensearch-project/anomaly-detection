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

package org.opensearch.ad.ml;

import static org.junit.Assert.assertEquals;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class ThresholdingResultTests {

    private double grade = 1.;
    private double confidence = 0.5;
    double score = 1.;

    private ThresholdingResult thresholdingResult = new ThresholdingResult(grade, confidence, score);

    @Test
    public void getters_returnExcepted() {
        assertEquals(grade, thresholdingResult.getGrade(), 1e-8);
        assertEquals(confidence, thresholdingResult.getConfidence(), 1e-8);
    }

    private Object[] equalsData() {
        return new Object[] {
            new Object[] { thresholdingResult, null, false },
            new Object[] { thresholdingResult, thresholdingResult, true },
            new Object[] { thresholdingResult, 1, false },
            new Object[] { thresholdingResult, new ThresholdingResult(grade, confidence, score), true },
            new Object[] { thresholdingResult, new ThresholdingResult(grade + 1, confidence, score), false },
            new Object[] { thresholdingResult, new ThresholdingResult(grade, confidence + 1, score), false },
            new Object[] { thresholdingResult, new ThresholdingResult(grade + 1, confidence + 1, score), false }, };
    }

    @Test
    @Parameters(method = "equalsData")
    public void equals_returnExpected(ThresholdingResult result, Object other, boolean expected) {
        assertEquals(expected, result.equals(other));
    }

    private Object[] hashCodeData() {
        return new Object[] {
            new Object[] { thresholdingResult, new ThresholdingResult(grade, confidence, score), true },
            new Object[] { thresholdingResult, new ThresholdingResult(grade + 1, confidence, score), false },
            new Object[] { thresholdingResult, new ThresholdingResult(grade, confidence + 1, score), false },
            new Object[] { thresholdingResult, new ThresholdingResult(grade + 1, confidence + 1, score), false }, };
    }

    @Test
    @Parameters(method = "hashCodeData")
    public void hashCode_returnExpected(ThresholdingResult result, ThresholdingResult other, boolean expected) {
        assertEquals(expected, result.hashCode() == other.hashCode());
    }
}
