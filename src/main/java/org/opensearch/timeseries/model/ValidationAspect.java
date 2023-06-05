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

package org.opensearch.timeseries.model;

import java.util.Collection;
import java.util.Set;

import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.timeseries.Name;
import org.opensearch.timeseries.constant.CommonName;

/**
 * Validation Aspect enum. There two types of validation types for validation API,
 * these correlate to two the possible params passed to validate API.
 * <ul>
 * <li><code>DETECTOR</code>:
 *     All the following validation checks that will be executed will be
 *     based on detector configuration settings. If any validation checks fail the AD Creation
 *     process will be blocked and the user will be indicated what fields caused the failure.
 * </ul>
 */
public enum ValidationAspect implements Name {
    DETECTOR(ADCommonName.DETECTOR_ASPECT),
    MODEL(CommonName.MODEL_ASPECT),
    FORECASTER(ForecastCommonName.FORECASTER_ASPECT);

    private String name;

    ValidationAspect(String name) {
        this.name = name;
    }

    /**
     * Get validation aspect
     *
     * @return name
     */
    @Override
    public String getName() {
        return name;
    }

    public static ValidationAspect getName(String name) {
        switch (name) {
            case ADCommonName.DETECTOR_ASPECT:
                return DETECTOR;
            case CommonName.MODEL_ASPECT:
                return MODEL;
            case ForecastCommonName.FORECASTER_ASPECT:
                return FORECASTER;
            default:
                throw new IllegalArgumentException("Unsupported validation aspects");
        }
    }

    public static Set<ValidationAspect> getNames(Collection<String> names) {
        return Name.getNameFromCollection(names, ValidationAspect::getName);
    }
}
