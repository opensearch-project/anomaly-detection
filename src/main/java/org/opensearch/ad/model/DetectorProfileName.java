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

public enum DetectorProfileName implements Name {
    STATE(CommonName.STATE),
    ERROR(CommonName.ERROR),
    COORDINATING_NODE(CommonName.COORDINATING_NODE),
    SHINGLE_SIZE(CommonName.SHINGLE_SIZE),
    TOTAL_SIZE_IN_BYTES(CommonName.TOTAL_SIZE_IN_BYTES),
    MODELS(CommonName.MODELS),
    INIT_PROGRESS(CommonName.INIT_PROGRESS),
    TOTAL_ENTITIES(CommonName.TOTAL_ENTITIES),
    ACTIVE_ENTITIES(CommonName.ACTIVE_ENTITIES),
    AD_TASK(CommonName.AD_TASK);

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
            case CommonName.STATE:
                return STATE;
            case CommonName.ERROR:
                return ERROR;
            case CommonName.COORDINATING_NODE:
                return COORDINATING_NODE;
            case CommonName.SHINGLE_SIZE:
                return SHINGLE_SIZE;
            case CommonName.TOTAL_SIZE_IN_BYTES:
                return TOTAL_SIZE_IN_BYTES;
            case CommonName.MODELS:
                return MODELS;
            case CommonName.INIT_PROGRESS:
                return INIT_PROGRESS;
            case CommonName.TOTAL_ENTITIES:
                return TOTAL_ENTITIES;
            case CommonName.ACTIVE_ENTITIES:
                return ACTIVE_ENTITIES;
            case CommonName.AD_TASK:
                return AD_TASK;
            default:
                throw new IllegalArgumentException("Unsupported profile types");
        }
    }

    public static Set<DetectorProfileName> getNames(Collection<String> names) {
        return Name.getNameFromCollection(names, DetectorProfileName::getName);
    }
}
