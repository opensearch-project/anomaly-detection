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

import java.util.Arrays;
import java.util.Objects;

/**
 * Data object containing results from RCF models.
 */
public class RcfResult {

    private final double score;
    private final double confidence;
    private final int forestSize;
    private final double[] attribution;
    private final long totalUpdates;
    private final double grade;

    /**
     * Constructor with all arguments.
     *
     * @param score RCF score
     * @param confidence RCF confidence
     * @param forestSize number of RCF trees used for the score
     * @param attribution anomaly score attribution
     * @param totalUpdates total updates
     * @param grade Anomaly grade
     */
    public RcfResult(double score, double confidence, int forestSize, double[] attribution, long totalUpdates, double grade) {
        this.score = score;
        this.confidence = confidence;
        this.forestSize = forestSize;
        this.attribution = attribution;
        this.totalUpdates = totalUpdates;
        this.grade = grade;
    }

    /**
     * Returns the RCF score.
     *
     * @return the RCF score
     */
    public double getScore() {
        return score;
    }

    /**
     * Returns the RCF confidence.
     *
     * @return the RCF confidence
     */
    public double getConfidence() {
        return confidence;
    }

    /**
     * Returns the number of RCF trees used for the score.
     *
     * @return the number of RCF trees used for the score
     */
    public int getForestSize() {
        return forestSize;
    }

    /**
     * Returns anomaly score attribution.
     *
     * @return anomaly score attribution
     */
    public double[] getAttribution() {
        return attribution;
    }

    public long getTotalUpdates() {
        return totalUpdates;
    }

    public double getGrade() {
        return grade;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        RcfResult that = (RcfResult) o;
        return Objects.equals(this.score, that.score)
            && Objects.equals(this.confidence, that.confidence)
            && Objects.equals(this.forestSize, that.forestSize)
            && Arrays.equals(this.attribution, that.attribution)
            && Objects.equals(this.totalUpdates, that.totalUpdates)
            && Objects.equals(this.grade, that.grade);
    }

    @Override
    public int hashCode() {
        return Objects.hash(score, confidence, forestSize, attribution, totalUpdates, grade);
    }
}
