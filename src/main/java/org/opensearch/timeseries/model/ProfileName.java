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
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.timeseries.Name;
import org.opensearch.timeseries.constant.CommonName;

public enum ProfileName implements Name {
    STATE(CommonName.STATE),
    ERROR(CommonName.ERROR),
    COORDINATING_NODE(CommonName.COORDINATING_NODE),
    TOTAL_SIZE_IN_BYTES(CommonName.TOTAL_SIZE_IN_BYTES),
    MODELS(CommonName.MODELS),
    INIT_PROGRESS(CommonName.INIT_PROGRESS),
    TOTAL_ENTITIES(CommonName.TOTAL_ENTITIES),
    ACTIVE_ENTITIES(CommonName.ACTIVE_ENTITIES),
    // AD only
    AD_TASK(ADCommonName.AD_TASK),
    // Forecast only
    FORECAST_TASK(ForecastCommonName.FORECAST_TASK);

    private String name;

    ProfileName(String name) {
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

    public static ProfileName getName(String name) {
        switch (name) {
            case CommonName.STATE:
                return STATE;
            case CommonName.ERROR:
                return ERROR;
            case CommonName.COORDINATING_NODE:
                return COORDINATING_NODE;
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
            case ADCommonName.AD_TASK:
                return AD_TASK;
            case ForecastCommonName.FORECAST_TASK:
                return FORECAST_TASK;
            default:
                throw new IllegalArgumentException(ADCommonMessages.UNSUPPORTED_PROFILE_TYPE);
        }
    }

    public static Set<ProfileName> getNames(Collection<String> names) {
        return Name.getNameFromCollection(names, ProfileName::getName);
    }
}
