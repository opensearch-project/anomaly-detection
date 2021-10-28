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

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.FeatureData;

/**
 * Data object containing thresholding results.
 */
public class ThresholdingResult {

    private final double grade;
    private final double confidence;
    private final double rcfScore;
    private long totalUpdates;

    // flag indicating if the anomaly is the start of an anomaly or part of a run of
    // anomalies
    private boolean startOfAnomaly;

    // flag indicating if the time stamp is in elevated score region to be
    // considered as anomaly
    private boolean inHighScoreRegion;

    /**
     * position of the anomaly vis a vis the current time (can be -ve) if anomaly is
     * detected late, which can and should happen sometime; for shingle size 1; this
     * is always 0
     */
    private int relativeIndex;

    // a flattened version denoting the basic contribution of each input variable
    private double[] relevantAttribution;

    // oldValues is related to relativeIndex and startOfAnomaly. Read the same
    // field comment on AnomalyResult.
    private double[] pastValues;

    // expected values, currently set to maximum 1 expected. In the future, we
    // might give different expected values with differently likelihood. So
    // the two-dimensional array allows us to future-proof our applications.
    // Also, expected values correspond to oldValues if present or current input
    // point otherwise. If oldValues is present, it will take effort to show this
    // on UX since we found an anomaly from the past (old values).
    private double[][] expectedValuesList;

    // likelihood values for the list
    private double[] likelihoodOfValues;

    // rcf score threshold at the time of writing a result
    private double threshold;

    // size of the forest
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
        this(grade, confidence, rcfScore, 0, false, false, 0, null, null, null, null, 0, 0);
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
        double[] pastValues,
        double[][] expectedValuesList,
        double[] likelihoodOfValues,
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
        this.relevantAttribution = currentTimeAttribution;
        this.pastValues = pastValues;
        this.expectedValuesList = expectedValuesList;
        this.likelihoodOfValues = likelihoodOfValues;
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

    public double[] getRelevantttribution() {
        return relevantAttribution;
    }

    public double[] getPastValues() {
        return pastValues;
    }

    public double[][] getExpectedValuesList() {
        return expectedValuesList;
    }

    public double[] getLikelihoodOfValues() {
        return likelihoodOfValues;
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
            && Arrays.equals(relevantAttribution, that.relevantAttribution)
            && Arrays.equals(pastValues, that.pastValues)
            && Arrays.deepEquals(expectedValuesList, that.expectedValuesList)
            && Arrays.equals(likelihoodOfValues, that.likelihoodOfValues)
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
                Arrays.hashCode(relevantAttribution),
                Arrays.hashCode(pastValues),
                Arrays.deepHashCode(expectedValuesList),
                Arrays.hashCode(likelihoodOfValues),
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
            .append("relevantAttribution", Arrays.toString(relevantAttribution))
            .append("oldValues", Arrays.toString(pastValues))
            .append("expectedValuesList", Arrays.deepToString(expectedValuesList))
            .append("likelihoodOfValues", Arrays.toString(likelihoodOfValues))
            .append("threshold", threshold)
            .append("forestSize", forestSize)
            .toString();
    }

    /**
    *
    * Convert ThresholdingResult to AnomalyResult
    *
    * @param detector Detector config
    * @param dataStartInstant data start time
    * @param dataEndInstant data end time
    * @param executionStartInstant  execution start time
    * @param executionEndInstant execution end time
    * @param featureData Feature data list
    * @param entity Entity attributes
    * @param schemaVersion Schema version
    * @param modelId Model Id
    * @param taskId Task Id
    * @param error Error
    * @return converted AnomalyResult
    */
    public AnomalyResult toAnomalyResult(
        AnomalyDetector detector,
        Instant dataStartInstant,
        Instant dataEndInstant,
        Instant executionStartInstant,
        Instant executionEndInstant,
        List<FeatureData> featureData,
        Entity entity,
        Integer schemaVersion,
        String modelId,
        String taskId,
        String error
    ) {
        return AnomalyResult
            .fromRawTRCFResult(
                detector.getDetectorId(),
                detector.getDetectorIntervalInMilliseconds(),
                taskId,
                rcfScore,
                grade,
                confidence,
                featureData,
                dataStartInstant,
                dataEndInstant,
                executionStartInstant,
                executionEndInstant,
                error,
                entity,
                detector.getUser(),
                schemaVersion,
                modelId,
                startOfAnomaly,
                inHighScoreRegion,
                relevantAttribution,
                relativeIndex,
                pastValues,
                expectedValuesList,
                likelihoodOfValues,
                threshold
            );
    }
}
