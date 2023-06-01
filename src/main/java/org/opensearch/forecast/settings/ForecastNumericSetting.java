/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.settings;

import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.common.settings.Setting;
import org.opensearch.timeseries.settings.DynamicNumericSetting;

public class ForecastNumericSetting extends DynamicNumericSetting {
    /**
     * Singleton instance
     */
    private static ForecastNumericSetting INSTANCE;

    /**
     * Settings name
     */
    public static final String CATEGORY_FIELD_LIMIT = "plugins.forecast.category_field_limit";

    private static final Map<String, Setting<?>> settings = unmodifiableMap(new HashMap<String, Setting<?>>() {
        {
            // how many categorical fields we support
            // The number of category field won't causes correctness issues for our
            // implementation, but can cause performance issues. The more categorical
            // fields, the larger of the forecast results, intermediate states, and
            // more expensive queries (e.g., to get top entities in preview API, we need
            // to use scripts in terms aggregation. The more fields, the slower the query).
            put(
                CATEGORY_FIELD_LIMIT,
                Setting.intSetting(CATEGORY_FIELD_LIMIT, 2, 0, 5, Setting.Property.NodeScope, Setting.Property.Dynamic)
            );
        }
    });

    ForecastNumericSetting(Map<String, Setting<?>> settings) {
        super(settings);
    }

    public static synchronized ForecastNumericSetting getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ForecastNumericSetting(settings);
        }
        return INSTANCE;
    }

    /**
     * @return the max number of categorical fields
     */
    public static int maxCategoricalFields() {
        return ForecastNumericSetting.getInstance().getSettingValue(ForecastNumericSetting.CATEGORY_FIELD_LIMIT);
    }
}
