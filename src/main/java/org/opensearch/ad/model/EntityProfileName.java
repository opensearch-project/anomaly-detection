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

public enum EntityProfileName implements Name {
    INIT_PROGRESS(ADCommonName.INIT_PROGRESS),
    ENTITY_INFO(ADCommonName.ENTITY_INFO),
    STATE(ADCommonName.STATE),
    MODELS(ADCommonName.MODELS);

    private String name;

    EntityProfileName(String name) {
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

    public static EntityProfileName getName(String name) {
        switch (name) {
            case ADCommonName.INIT_PROGRESS:
                return INIT_PROGRESS;
            case ADCommonName.ENTITY_INFO:
                return ENTITY_INFO;
            case ADCommonName.STATE:
                return STATE;
            case ADCommonName.MODELS:
                return MODELS;
            default:
                throw new IllegalArgumentException(CommonErrorMessages.UNSUPPORTED_PROFILE_TYPE);
        }
    }

    public static Set<EntityProfileName> getNames(Collection<String> names) {
        return Name.getNameFromCollection(names, EntityProfileName::getName);
    }
}
