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

import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.timeseries.Name;
import org.opensearch.timeseries.constant.CommonName;

public enum EntityProfileName implements Name {
    INIT_PROGRESS(CommonName.INIT_PROGRESS),
    ENTITY_INFO(CommonName.ENTITY_INFO),
    STATE(CommonName.STATE),
    MODELS(CommonName.MODELS);

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
            case CommonName.INIT_PROGRESS:
                return INIT_PROGRESS;
            case CommonName.ENTITY_INFO:
                return ENTITY_INFO;
            case CommonName.STATE:
                return STATE;
            case CommonName.MODELS:
                return MODELS;
            default:
                throw new IllegalArgumentException(ADCommonMessages.UNSUPPORTED_PROFILE_TYPE);
        }
    }

    public static Set<EntityProfileName> getNames(Collection<String> names) {
        return Name.getNameFromCollection(names, EntityProfileName::getName);
    }
}
