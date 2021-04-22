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

package com.amazon.opendistroforelasticsearch.ad.model;

import java.util.Collection;
import java.util.Set;

import com.amazon.opendistroforelasticsearch.ad.Name;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;

public enum ValidationAspect implements Name {
    DETECTOR(CommonName.DETECTOR),
    MODEL(CommonName.MODEL);

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
            case CommonName.MODEL:
                return MODEL;
            default:
                throw new IllegalArgumentException("Unsupported validation aspects");
        }
    }

    public static Set<ValidationAspect> getNames(Collection<String> names) {
        return Name.getNameFromCollection(names, ValidationAspect::getName);
    }
}
