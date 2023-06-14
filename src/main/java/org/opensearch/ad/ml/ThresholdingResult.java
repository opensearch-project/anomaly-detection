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
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.FeatureData;

/**
 * Data object containing thresholding results.
 */
public class ThresholdingResult {

    private final double grade;
    private final double confidence;
    private final double rcfScore;
    private long totalUpdates;

    /**
     * position of the anomaly vis a vis the current time (can be -ve) if anomaly is
     * detected late, which can and should happen sometime; for shingle size 1; this
     * is always 0.
     *
     * For example, current shingle is
        [
        6819.0,
        2375.3333333333335,
        0.0,
        49882.0,
        92070.0,
        5084.0,
        2072.809523809524,
        0.0,
        43529.0,
        91169.0,
        8129.0,
        2582.892857142857,
        12.0,
        54241.0,
        84596.0,
        11174.0,
        3092.9761904761904,
        24.0,
        64952.0,
        78024.0,
        14220.0,
        3603.059523809524,
        37.0,
        75664.0,
        71451.0,
        17265.0,
        4113.142857142857,
        49.0,
        86376.0,
        64878.0,
        16478.0,
        3761.4166666666665,
        37.0,
        78990.0,
        70057.0,
        15691.0,
        3409.690476190476,
        24.0,
        71604.0,
        75236.0
        ],
     * If rcf returns relativeIndex is -2, baseDimension is 5, we look back baseDimension * 2 and get the
     * culprit input that triggers anomaly:
        [17265.0,
         4113.142857142857,
         49.0,
         86376.0,
         64878.0
        ],
     */
    private int relativeIndex;

    // a flattened version denoting the basic contribution of each input variable
    private double[] relevantAttribution;

    // pastValues is related to relativeIndex and startOfAnomaly. Read the same
    // field comment on AnomalyResult.
    private double[] pastValues;

    /*
     * The expected value is only calculated for anomalous detection intervals,
     * and will generate expected value for each feature if detector has multiple
     * features.
     * Currently we expect one set of expected values. In the future, we
     * might give different expected values with differently likelihood. So
     * the two-dimensional array allows us to future-proof our applications.
     * Also, expected values correspond to pastValues if present or current input
     * point otherwise. If pastValues is present, we can add a text on UX to explain
     * we found an anomaly from the past.
     Example:
     "expected_value": [{
        "likelihood": 0.8,
        "value_list": [{
                "feature_id": "blah",
                "value": 1
            },
            {
                "feature_id": "blah2",
                "value": 1
            }
        ]
    }]*/
    private double[][] expectedValuesList;

    // likelihood values for the list.
    // There will be one likelihood value that spans a single set of expected values.
    // For now, only one likelihood value should be expected as there is only
    // one set of expected values.
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
        this(grade, confidence, rcfScore, 0, 0, null, null, null, null, 0, 0);
    }

    public ThresholdingResult(
        double grade,
        double confidence,
        double rcfScore,
        long totalUpdates,
        int relativeIndex,
        double[] relevantAttribution,
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
        this.relativeIndex = relativeIndex;
        this.relevantAttribution = relevantAttribution;
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

    public int getRelativeIndex() {
        return relativeIndex;
    }

    public double[] getRelevantAttribution() {
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
            .append("relativeIndex", relativeIndex)
            .append("relevantAttribution", Arrays.toString(relevantAttribution))
            .append("pastValues", Arrays.toString(pastValues))
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
                detector.getId(),
                detector.getIntervalInMilliseconds(),
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
                relevantAttribution,
                relativeIndex,
                pastValues,
                expectedValuesList,
                likelihoodOfValues,
                threshold
            );
    }
}
