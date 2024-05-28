/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.settings;

import static java.util.Collections.unmodifiableMap;
import static org.opensearch.common.settings.Setting.Property.Dynamic;
import static org.opensearch.common.settings.Setting.Property.NodeScope;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.common.settings.Setting;
import org.opensearch.timeseries.settings.DynamicNumericSetting;

public class ForecastEnabledSetting extends DynamicNumericSetting {

    /**
     * Singleton instance
     */
    private static ForecastEnabledSetting INSTANCE;

    /**
     * Settings name
     */
    public static final String FORECAST_ENABLED = "plugins.forecast.enabled";

    public static final boolean enabled = false;

    public static final Map<String, Setting<?>> settings = unmodifiableMap(new HashMap<String, Setting<?>>() {
        {
            /**
             * forecast enable/disable setting
             */
            // TODO: enable forecasting by default. Currently disabled.
            put(FORECAST_ENABLED, Setting.boolSetting(FORECAST_ENABLED, false, NodeScope, Dynamic));
        }
    });

    private ForecastEnabledSetting(Map<String, Setting<?>> settings) {
        super(settings);
    }

    public static synchronized ForecastEnabledSetting getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ForecastEnabledSetting(settings);
        }
        return INSTANCE;
    }

    /**
     * Whether forecasting is enabled.  If disabled, time series plugin rejects RESTful requests about forecasting and stop all forecasting jobs.
     * @return whether forecasting is enabled.
     */
    public static boolean isForecastEnabled() {
        //return ForecastEnabledSetting.getInstance().getSettingValue(ForecastEnabledSetting.FORECAST_ENABLED);
        // TODO: enable forecasting before released
        return enabled;
    }
}
