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
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.randomcutforest.ERCF;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.config.Precision;

public class ExtendedRandomCutForestTests {

    private Random random = new Random();

    @Test
    public void constructor_succeed() {
        new ExtendedRandomCutForest(
            RandomCutForest
                .builder()
                .compact(true)
                .dimensions(8)
                .numberOfTrees(100)
                .shingleSize(8)
                .sampleSize(256)
                .precision(Precision.FLOAT_32),
            0.005
        );
    }

    @Test
    public void process_output_anomaly() {
        int shingle = 8;
        int sample = 256;
        ExtendedRandomCutForest ercf = new ExtendedRandomCutForest(
            RandomCutForest
                .builder()
                .compact(true)
                .dimensions(shingle)
                .numberOfTrees(100)
                .shingleSize(shingle)
                .sampleSize(sample)
                .precision(Precision.FLOAT_32),
            0.005
        );

        int normalHigh = 10;
        int normalLow = 0;
        for (int i = 0; i < sample; i++) {
            AnomalyDescriptor result = ercf.process(this.random.doubles(shingle, normalLow, normalHigh).toArray());
        }

        double[] input = this.random.doubles(shingle, normalLow, normalHigh).toArray();
        int anomalyIndex = 7;
        input[anomalyIndex] = normalHigh * 2;
        AnomalyDescriptor result = ercf.process(input);

        assertTrue(result.getAnomalyGrade() > 0);
        assertTrue(result.getFlattenedAttribution()[0] > result.getRcfScore() / shingle);
        Arrays.stream(result.getExpectedValuesList()[0]).forEach(v -> assertTrue(v >= normalLow && v <= normalHigh));
    }
}
