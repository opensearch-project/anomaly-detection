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

package org.opensearch.ad.model;

import java.util.Collection;
import java.util.Set;

import org.opensearch.ad.Name;
import org.opensearch.ad.constant.CommonName;

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
    DETECTOR(CommonName.DETECTOR);

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
            case CommonName.DETECTOR:
                return DETECTOR;
            default:
                throw new IllegalArgumentException("Unsupported validation aspects");
        }
    }

    public static Set<ValidationAspect> getNames(Collection<String> names) {
        return Name.getNameFromCollection(names, ValidationAspect::getName);
    }
}
