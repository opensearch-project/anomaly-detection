/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import java.util.List;
import java.util.Map;

import org.opensearch.ad.model.ImputedFeatureResult;
import org.opensearch.timeseries.dataprocessor.ImputationMethod;
import org.opensearch.timeseries.dataprocessor.ImputationOption;
import org.opensearch.timeseries.model.Config;

import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;

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
}
