/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.ImputedFeatureResult;
import org.opensearch.forecast.ml.RCFCasterResult;
import org.opensearch.timeseries.dataprocessor.ImputationMethod;
import org.opensearch.timeseries.dataprocessor.ImputationOption;
import org.opensearch.timeseries.model.Config;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ForecastDescriptor;

public class ModelUtil {
    public static ImputedFeatureResult calculateImputedFeatures(
        AnomalyDescriptor anomalyDescriptor,
        double[] point,
        boolean isImputed,
        Config config
    ) {
        int inputLength = anomalyDescriptor.getInputLength();
        boolean[] isFeatureImputed = null;
        double[] actual = point;

        if (isImputed) {
            actual = new double[inputLength];
            isFeatureImputed = new boolean[inputLength];

            ImputationOption imputationOption = config.getImputationOption();
            if (imputationOption != null && imputationOption.getMethod() == ImputationMethod.ZERO) {
                for (int i = 0; i < point.length; i++) {
                    if (Double.isNaN(point[i])) {
                        isFeatureImputed[i] = true;
                        actual[i] = 0;
                    }
                }
            } else if (imputationOption != null && imputationOption.getMethod() == ImputationMethod.FIXED_VALUES) {
                Map<String, Double> defaultFills = imputationOption.getDefaultFill();
                List<String> enabledFeatureNames = config.getEnabledFeatureNames();
                for (int i = 0; i < point.length; i++) {
                    if (Double.isNaN(point[i])) {
                        isFeatureImputed[i] = true;
                        actual[i] = defaultFills.get(enabledFeatureNames.get(i));
                    }
                }
            } else {
                float[] rcfPoint = anomalyDescriptor.getRCFPoint();
                if (rcfPoint == null) {
                    return new ImputedFeatureResult(isFeatureImputed, actual);
                }
                float[] transformedInput = new float[inputLength];
                System.arraycopy(rcfPoint, rcfPoint.length - inputLength, transformedInput, 0, inputLength);

                double[] scale = anomalyDescriptor.getScale();
                double[] shift = anomalyDescriptor.getShift();

                for (int i = 0; i < point.length; i++) {
                    if (Double.isNaN(point[i])) {
                        isFeatureImputed[i] = true;
                        actual[i] = (transformedInput[i] * scale[i]) + shift[i];
                    }
                }
            }
        }

        return new ImputedFeatureResult(isFeatureImputed, actual);
    }

    public static RCFCasterResult toResult(RandomCutForest forecast, ForecastDescriptor castDescriptor, double[] point, boolean isImputed) {
        if (castDescriptor instanceof ForecastDescriptor) {
            ForecastDescriptor forecastDescriptor = castDescriptor;
            // Use forecastDescriptor in the rest of your method
            return new RCFCasterResult(
                forecastDescriptor.getTimedForecast().rangeVector,
                forecastDescriptor.getDataConfidence(),
                forecast.getTotalUpdates(),
                forecastDescriptor.getRCFScore()
            );
        } else {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Unsupported type of AnomalyDescriptor : %s", castDescriptor));
        }
    }

    public static ThresholdingResult toResult(
        RandomCutForest rcf,
        AnomalyDescriptor anomalyDescriptor,
        double[] point,
        boolean isImputed,
        Config config
    ) {
        ImputedFeatureResult result = ModelUtil.calculateImputedFeatures(anomalyDescriptor, point, isImputed, config);

        return new ThresholdingResult(
            anomalyDescriptor.getAnomalyGrade(),
            anomalyDescriptor.getDataConfidence(),
            anomalyDescriptor.getRCFScore(),
            anomalyDescriptor.getTotalUpdates(),
            anomalyDescriptor.getRelativeIndex(),
            normalizeAttribution(rcf, anomalyDescriptor.getRelevantAttribution()),
            anomalyDescriptor.getPastValues(),
            anomalyDescriptor.getExpectedValuesList(),
            anomalyDescriptor.getLikelihoodOfValues(),
            anomalyDescriptor.getThreshold(),
            anomalyDescriptor.getNumberOfTrees(),
            result.getActual(),
            result.getIsFeatureImputed()
        );
    }

    /**
     * normalize total attribution to 1
     *
     * @param forest rcf accessor
     * @param rawAttribution raw attribution scores.  Can be null when
     * 1) the anomaly grade is 0;
     * 2) there are missing values and we are using differenced transforms.
     * Read RCF's ImputePreprocessor.postProcess.
     *
     * @return normalized attribution
     */
    public static double[] normalizeAttribution(RandomCutForest forest, double[] rawAttribution) {
        if (forest == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Empty forest"));
        }
        // rawAttribution is null when anomaly grade is less than or equals to 0
        // need to create an empty array for bwc because the old node expects an non-empty array
        double[] attribution = createEmptyAttribution(forest);
        if (rawAttribution != null && rawAttribution.length > 0) {
            double sum = Arrays.stream(rawAttribution).sum();
            // avoid dividing by zero error
            if (sum > 0) {
                if (rawAttribution.length != attribution.length) {
                    throw new IllegalArgumentException(
                        String
                            .format(
                                Locale.ROOT,
                                "Unexpected attribution array length: expected %d but is %d",
                                attribution.length,
                                rawAttribution.length
                            )
                    );
                }
                int numFeatures = rawAttribution.length;
                attribution = new double[numFeatures];
                for (int i = 0; i < numFeatures; i++) {
                    attribution[i] = rawAttribution[i] / sum;
                }
            }
        }

        return attribution;
    }

    public static double[] createEmptyAttribution(RandomCutForest forest) {
        int shingleSize = forest.getShingleSize();
        if (shingleSize <= 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "zero shingle size"));
        }
        int baseDimensions = forest.getDimensions() / shingleSize;
        return new double[baseDimensions];
    }
}
