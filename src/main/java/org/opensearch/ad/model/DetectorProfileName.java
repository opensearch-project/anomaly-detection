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
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.constant.CommonErrorMessages;

public enum DetectorProfileName implements Name {
    STATE(ADCommonName.STATE),
    ERROR(ADCommonName.ERROR),
    COORDINATING_NODE(ADCommonName.COORDINATING_NODE),
    SHINGLE_SIZE(ADCommonName.SHINGLE_SIZE),
    TOTAL_SIZE_IN_BYTES(ADCommonName.TOTAL_SIZE_IN_BYTES),
    MODELS(ADCommonName.MODELS),
    INIT_PROGRESS(ADCommonName.INIT_PROGRESS),
    TOTAL_ENTITIES(ADCommonName.TOTAL_ENTITIES),
    ACTIVE_ENTITIES(ADCommonName.ACTIVE_ENTITIES),
    AD_TASK(ADCommonName.AD_TASK);

    private String name;

    DetectorProfileName(String name) {
        this.name = name;
    }

    /**
     * Get profile name
     *
     * @return name
     */
    @Override
    public String getName() {
        return name;
    }

    public static DetectorProfileName getName(String name) {
        switch (name) {
            case ADCommonName.STATE:
                return STATE;
            case ADCommonName.ERROR:
                return ERROR;
            case ADCommonName.COORDINATING_NODE:
                return COORDINATING_NODE;
            case ADCommonName.SHINGLE_SIZE:
                return SHINGLE_SIZE;
            case ADCommonName.TOTAL_SIZE_IN_BYTES:
                return TOTAL_SIZE_IN_BYTES;
            case ADCommonName.MODELS:
                return MODELS;
            case ADCommonName.INIT_PROGRESS:
                return INIT_PROGRESS;
            case ADCommonName.TOTAL_ENTITIES:
                return TOTAL_ENTITIES;
            case ADCommonName.ACTIVE_ENTITIES:
                return ACTIVE_ENTITIES;
            case ADCommonName.AD_TASK:
                return AD_TASK;
            default:
                throw new IllegalArgumentException(CommonErrorMessages.UNSUPPORTED_PROFILE_TYPE);
        }
    }

    public static Set<DetectorProfileName> getNames(Collection<String> names) {
        return Name.getNameFromCollection(names, DetectorProfileName::getName);
    }
}
