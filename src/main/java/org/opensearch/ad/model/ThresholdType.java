/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

/**
 * Enumerates the types of thresholds used in anomaly detection.
 *
 * This enumeration defines various types of thresholds that dictate
 * how anomalies are identified in comparison to actual and expected values.
 * These thresholds include direct comparisons of actual and expected values,
 * as well as differences and ratios between these values from specified directions.
 */
public enum ThresholdType {
    /**
     * Specifies a threshold for ignoring anomalies where the actual value
     * exceeds the expected value by a certain margin.
     *
     * Assume a represents the actual value and b signifies the expected value.
     * IGNORE_SIMILAR_FROM_ABOVE implies the anomaly should be disregarded if a-b
     * is less than or equal to ignoreSimilarFromAbove.
     */
    ACTUAL_OVER_EXPECTED_MARGIN("a margin by which the actual values exceed the expected one"),

    /**
     * Specifies a threshold for ignoring anomalies where the actual value
     * is below the expected value by a certain margin.
     *
     * Assume a represents the actual value and b signifies the expected value.
     * Likewise, IGNORE_SIMILAR_FROM_BELOW
     * implies the anomaly should be disregarded if b-a is less than or equal to
     * ignoreSimilarFromBelow.
     */
    EXPECTED_OVER_ACTUAL_MARGIN("a margin by which expected values exceed actual ones"),

    /**
     * Specifies a threshold for ignoring anomalies based on the ratio of
     * the difference to the actual value when the actual value exceeds
     * the expected value.
     *
     * Assume a represents the actual value and b signifies the expected value.
     * The variable IGNORE_NEAR_EXPECTED_FROM_ABOVE_BY_RATIO presumably implies the
     * anomaly should be disregarded if the ratio of the deviation from the actual
     * to the expected (a-b)/|a| is less than or equal to IGNORE_NEAR_EXPECTED_FROM_ABOVE_BY_RATIO.
     */
    ACTUAL_OVER_EXPECTED_RATIO("the ratio of the actual value over the expected value"),

    /**
     * Specifies a threshold for ignoring anomalies based on the ratio of
     * the difference to the actual value when the actual value is below
     * the expected value.
     *
     * Assume a represents the actual value and b signifies the expected value.
     * Likewise, IGNORE_NEAR_EXPECTED_FROM_BELOW_BY_RATIO appears to indicate that the anomaly
     * should be ignored if the ratio of the deviation from the expected to the actual
     * (b-a)/|a| is less than or equal to ignoreNearExpectedFromBelowByRatio.
     */
    EXPECTED_OVER_ACTUAL_RATIO("the ratio of the expected value over the actual value"),

    /**
      * Specifies a threshold for ignoring anomalies based on whether the actual value
      * is over the expected value returned from the model.
      */
    ACTUAL_IS_OVER_EXPECTED("the actual value is over the expected value"),

    /**
    * Specifies a threshold for ignoring anomalies based on whether the actual value
    * is below the expected value returned from the model.
     * */
    ACTUAL_IS_BELOW_EXPECTED("the actual value is below the expected value");

    private final String description;

    /**
     * Constructs a ThresholdType with a descriptive name.
     *
     * @param description The human-readable description of the threshold type.
     */
    ThresholdType(String description) {
        this.description = description;
    }

    /**
     * Retrieves the description of the threshold type.
     *
     * @return A string describing the threshold type.
     */
    public String getDescription() {
        return description;
    }
}
