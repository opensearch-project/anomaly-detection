/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import java.util.Collection;
import java.util.Set;

import org.opensearch.forecast.model.Forecaster;
import org.opensearch.timeseries.Name;
import org.opensearch.timeseries.constant.CommonMessages;

public enum ForecastSuggestName implements Name {
    INTERVAL(Forecaster.FORECAST_INTERVAL_FIELD),
    HORIZON(Forecaster.HORIZON_FIELD),
    HISTORY(Forecaster.HISTORY_INTERVAL_FIELD),
    WINDOW_DELAY(Forecaster.WINDOW_DELAY_FIELD);

    private String name;

    ForecastSuggestName(String name) {
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

    public static ForecastSuggestName getName(String name) {
        switch (name) {
            case Forecaster.FORECAST_INTERVAL_FIELD:
                return INTERVAL;
            case Forecaster.HORIZON_FIELD:
                return HORIZON;
            case Forecaster.HISTORY_INTERVAL_FIELD:
                return HISTORY;
            case Forecaster.WINDOW_DELAY_FIELD:
                return WINDOW_DELAY;
            default:
                throw new IllegalArgumentException(CommonMessages.NOT_EXISTENT_SUGGEST_TYPE);
        }
    }

    public static Set<ForecastSuggestName> getNames(Collection<String> names) {
        return Name.getNameFromCollection(names, ForecastSuggestName::getName);
    }
}
