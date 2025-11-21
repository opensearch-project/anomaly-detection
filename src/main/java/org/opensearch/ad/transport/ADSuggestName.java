/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.util.Collection;
import java.util.Set;

import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.timeseries.Name;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.Config;

public enum ADSuggestName implements Name {
    INTERVAL(AnomalyDetector.DETECTION_INTERVAL_FIELD),
    HISTORY(Config.HISTORY_INTERVAL_FIELD),
    WINDOW_DELAY(Config.WINDOW_DELAY_FIELD);

    private String name;

    ADSuggestName(String name) {
        this.name = name;
    }

    /**
     * Get suggest name
     *
     * @return name
     */
    @Override
    public String getName() {
        return name;
    }

    public static ADSuggestName getName(String name) {
        switch (name) {
            case AnomalyDetector.DETECTION_INTERVAL_FIELD:
                return INTERVAL;
            case Config.HISTORY_INTERVAL_FIELD:
                return HISTORY;
            case Config.WINDOW_DELAY_FIELD:
                return WINDOW_DELAY;
            default:
                throw new IllegalArgumentException(CommonMessages.NOT_EXISTENT_SUGGEST_TYPE);
        }
    }

    public static Set<ADSuggestName> getNames(Collection<String> names) {
        return Name.getNameFromCollection(names, ADSuggestName::getName);
    }
}
