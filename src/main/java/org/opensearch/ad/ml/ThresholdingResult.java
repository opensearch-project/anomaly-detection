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

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * Data object containing thresholding results.
 */
public class ThresholdingResult {

    private final double grade;
    private final double confidence;
    private final double rcfScore;
    private long totalUpdates;
    private boolean startOfAnomaly;
    private boolean inHighScoreRegion;
    private int relativeIndex;
    private double[] currentTimeAttribution;
    private double[] oldValues;
    private double[][] expectedValuesList;
    private double threshold;
    private int forestSize;

    /**
     * Constructor for default empty value or backward compatibility.
     * In terms of bwc, when an old node sends request for threshold results,
     * we need to return only what they understand.
     *
     * @param grade anomaly grade
     * @param confidence confidence for the grade
     * @param rcfScore rcf score associated with the grade and confidence. Used
     *   by multi-entity detector to differentiate whether the result is worth
     *   saving or not.
     */
    public ThresholdingResult(double grade, double confidence, double rcfScore) {
        this(grade, confidence, rcfScore, 0, false, false, 0, null, null, null, 0, 0);
    }

    public ThresholdingResult(
        double grade,
        double confidence,
        double rcfScore,
        long totalUpdates,
        boolean startOfAnomaly,
        boolean inHighScoreRegion,
        int relativeIndex,
        double[] currentTimeAttribution,
        double[] oldValues,
        double[][] expectedValuesList,
        double threshold,
        int forestSize
    ) {
        this.grade = grade;
        this.confidence = confidence;
        this.rcfScore = rcfScore;
        this.totalUpdates = totalUpdates;
        this.startOfAnomaly = startOfAnomaly;
        this.inHighScoreRegion = inHighScoreRegion;
        this.relativeIndex = relativeIndex;
        this.currentTimeAttribution = currentTimeAttribution;
        this.oldValues = oldValues;
        this.expectedValuesList = expectedValuesList;
        this.threshold = threshold;
        this.forestSize = forestSize;
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

    public long getTotalUpdates() {
        return totalUpdates;
    }

    public boolean isStartOfAnomaly() {
        return startOfAnomaly;
    }

    public boolean isInHighScoreRegion() {
        return inHighScoreRegion;
    }

    public int getRelativeIndex() {
        return relativeIndex;
    }

    public double[] getCurrentTimeAttribution() {
        return currentTimeAttribution;
    }

    public double[] getOldValues() {
        return oldValues;
    }

    public double[][] getExpectedValuesList() {
        return expectedValuesList;
    }

    public double getThreshold() {
        return threshold;
    }

    public int getForestSize() {
        return forestSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ThresholdingResult that = (ThresholdingResult) o;
        return this.grade == that.grade
            && this.confidence == that.confidence
            && this.rcfScore == that.rcfScore
            && this.totalUpdates == that.totalUpdates
            && this.startOfAnomaly == that.startOfAnomaly
            && this.inHighScoreRegion == that.inHighScoreRegion
            && this.relativeIndex == that.relativeIndex
            && Arrays.equals(currentTimeAttribution, currentTimeAttribution)
            && Arrays.equals(oldValues, oldValues)
            && Arrays.deepEquals(expectedValuesList, expectedValuesList)
            && threshold == that.threshold
            && forestSize == that.forestSize;
    }

    @Override
    public int hashCode() {
        return Objects
            .hash(
                grade,
                confidence,
                rcfScore,
                totalUpdates,
                startOfAnomaly,
                inHighScoreRegion,
                relativeIndex,
                Arrays.hashCode(currentTimeAttribution),
                Arrays.hashCode(oldValues),
                Arrays.deepHashCode(expectedValuesList),
                threshold,
                forestSize
            );
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("grade", grade)
            .append("confidence", confidence)
            .append("rcfScore", rcfScore)
            .append("totalUpdates", totalUpdates)
            .append("startOfAnomaly", startOfAnomaly)
            .append("inHighScoreRegion", inHighScoreRegion)
            .append("relativeIndex", relativeIndex)
            .append("currentTimeAttribution", Arrays.toString(currentTimeAttribution))
            .append("oldValues", Arrays.toString(oldValues))
            .append("expectedValuesList", Arrays.deepToString(expectedValuesList))
            .append("threshold", threshold)
            .append("forestSize", forestSize)
            .toString();
    }
}
