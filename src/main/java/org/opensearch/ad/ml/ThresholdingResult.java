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

import java.util.Objects;

/**
 * Data object containing thresholding results.
 */
public class ThresholdingResult {

    private final double grade;
    private final double confidence;
    private final double rcfScore;

    /**
     * Constructor with all arguments.
     *
     * @param grade anomaly grade
     * @param confidence confidence for the grade
     * @param rcfScore rcf score associated with the grade and confidence. Used
     *   by multi-entity detector to differentiate whether the result is worth
     *   saving or not.
     */
    public ThresholdingResult(double grade, double confidence, double rcfScore) {
        this.grade = grade;
        this.confidence = confidence;
        this.rcfScore = rcfScore;
    }

    /**
     * Returns the anomaly grade.
     *
     * @return the anoamly grade
     */
    public double getGrade() {
        return grade;
    }

    /**
     * Returns the confidence for the grade.
     *
     * @return confidence for the grade
     */
    public double getConfidence() {
        return confidence;
    }

    public double getRcfScore() {
        return rcfScore;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ThresholdingResult that = (ThresholdingResult) o;
        return Objects.equals(this.grade, that.grade)
            && Objects.equals(this.confidence, that.confidence)
            && Objects.equals(this.rcfScore, that.rcfScore);
    }

    @Override
    public int hashCode() {
        return Objects.hash(grade, confidence, rcfScore);
    }
}
