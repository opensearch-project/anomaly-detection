/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.ml;

import java.util.List;

import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Condition;
import org.opensearch.ad.model.Rule;
import org.opensearch.ad.model.ThresholdType;

/**
 * The IgnoreSimilarExtractor class provides functionality to process anomaly detection rules,
 * specifically focusing on extracting threshold values for various conditions to ignore
 * similar anomalies. It supports handling conditions based on absolute values and ratios,
 * distinguishing between thresholds that specify ignoring similarities from above or below
 * a certain value.
 */
public class IgnoreSimilarExtractor {

    // Define a class to hold the arrays
    public static class ThresholdArrays {
        public double[] ignoreSimilarFromAbove;
        public double[] ignoreSimilarFromBelow;
        public double[] ignoreSimilarFromAboveByRatio;
        public double[] ignoreSimilarFromBelowByRatio;

        public ThresholdArrays(
            double[] ignoreSimilarFromAbove,
            double[] ignoreSimilarFromBelow,
            double[] ignoreSimilarFromAboveByRatio,
            double[] ignoreSimilarFromBelowByRatio
        ) {
            this.ignoreSimilarFromAbove = ignoreSimilarFromAbove;
            this.ignoreSimilarFromBelow = ignoreSimilarFromBelow;
            this.ignoreSimilarFromAboveByRatio = ignoreSimilarFromAboveByRatio;
            this.ignoreSimilarFromBelowByRatio = ignoreSimilarFromBelowByRatio;
        }
    }

    public static ThresholdArrays processDetectorRules(AnomalyDetector detector) {
        List<String> featureNames = detector.getEnabledFeatureNames();
        int baseDimension = featureNames.size();
        Ref<double[]> ignoreSimilarFromAbove = Ref.of(null);
        Ref<double[]> ignoreSimilarFromBelow = Ref.of(null);
        Ref<double[]> ignoreSimilarFromAboveByRatio = Ref.of(null);
        Ref<double[]> ignoreSimilarFromBelowByRatio = Ref.of(null);

        List<Rule> rules = detector.getRules();
        if (rules != null) {
            for (Rule rule : rules) {
                for (Condition condition : rule.getConditions()) {
                    if (condition.getThresholdType() != ThresholdType.ACTUAL_IS_BELOW_EXPECTED
                        && condition.getThresholdType() != ThresholdType.ACTUAL_IS_OVER_EXPECTED) {
                        processCondition(
                            condition,
                            featureNames,
                            baseDimension,
                            ignoreSimilarFromAbove,
                            ignoreSimilarFromBelow,
                            ignoreSimilarFromAboveByRatio,
                            ignoreSimilarFromBelowByRatio
                        );
                    }
                }
            }
        }

        // Return a new ThresholdArrays instance containing the processed arrays
        return new ThresholdArrays(
            ignoreSimilarFromAbove.value,
            ignoreSimilarFromBelow.value,
            ignoreSimilarFromAboveByRatio.value,
            ignoreSimilarFromBelowByRatio.value
        );
    }

    private static class Ref<T> {
        public T value;

        private Ref(T value) {
            this.value = value;
        }

        public static <T> Ref<T> of(T value) {
            return new Ref<>(value);
        }
    }

    private static void processCondition(
        Condition condition,
        List<String> featureNames,
        int baseDimension,
        Ref<double[]> ignoreSimilarFromAbove,
        Ref<double[]> ignoreSimilarFromBelow,
        Ref<double[]> ignoreSimilarFromAboveByRatio,
        Ref<double[]> ignoreSimilarFromBelowByRatio
    ) {
        String featureName = condition.getFeatureName();
        int featureIndex = featureNames.indexOf(featureName);

        ThresholdType thresholdType = condition.getThresholdType();
        Double value = condition.getValue();
        if (value == null) {
            value = 0d;
        }

        switch (thresholdType) {
            case ACTUAL_OVER_EXPECTED_MARGIN:
                updateThresholdValue(baseDimension, ignoreSimilarFromAbove, featureIndex, value);
                break;
            case EXPECTED_OVER_ACTUAL_MARGIN:
                updateThresholdValue(baseDimension, ignoreSimilarFromBelow, featureIndex, value);
                break;
            case ACTUAL_OVER_EXPECTED_RATIO:
                updateThresholdValue(baseDimension, ignoreSimilarFromAboveByRatio, featureIndex, value);
                break;
            case EXPECTED_OVER_ACTUAL_RATIO:
                updateThresholdValue(baseDimension, ignoreSimilarFromBelowByRatio, featureIndex, value);
                break;
            default:
                break;
        }
    }

    private static void updateThresholdValue(int baseDimension, Ref<double[]> thresholdArrayRef, int featureIndex, double value) {
        if (thresholdArrayRef.value == null) {
            thresholdArrayRef.value = new double[baseDimension];
        }
        thresholdArrayRef.value[featureIndex] = value;
    }
}
