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
public class PMMLResultTests {
    private boolean outlier = false;
    private double decisionFunction = 0.1;
    private PMMLResult pmmlResult = new PMMLResult(outlier, decisionFunction);

    @Test
    public void getters_returnExcepted() {
        assertEquals(outlier, pmmlResult.getOutlier());
        assertEquals(decisionFunction, pmmlResult.getDecisionFunction(), 0.001);
    }

    private Object[] equalsData() {
        return new Object[] {
            new Object[] { pmmlResult, null, false },
            new Object[] { pmmlResult, pmmlResult, true },
            new Object[] { pmmlResult, 1, false },
            new Object[] { pmmlResult, new PMMLResult(outlier, decisionFunction), true },
            new Object[] { pmmlResult, new PMMLResult(true, -0.1), false },
            new Object[] { pmmlResult, new PMMLResult(false, 0.11), false } };
    }

    @Test
    @Parameters(method = "equalsData")
    public void equals_returnExpected(PMMLResult result, Object other, boolean expected) {
        assertEquals(expected, result.equals(other));
    }

    private Object[] hashCodeData() {
        return new Object[] {
            new Object[] { pmmlResult, new PMMLResult(outlier, decisionFunction), true },
            new Object[] { pmmlResult, new PMMLResult(false, 0.1), true },
            new Object[] { pmmlResult, new PMMLResult(false, 0.11), false },
            new Object[] { pmmlResult, new PMMLResult(true, -0.1), false } };
    }

    @Test
    @Parameters(method = "hashCodeData")
    public void hashCode_returnExpected(PMMLResult result, PMMLResult other, boolean expected) {
        assertEquals(expected, result.hashCode() == other.hashCode());
    }
}
