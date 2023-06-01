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

    public static final String FORECAST_BREAKER_ENABLED = "plugins.forecast.breaker.enabled";

    public static final String FORECAST_DOOR_KEEPER_IN_CACHE_ENABLED = "plugins.forecast.door_keeper_in_cache.enabled";;

    public static final Map<String, Setting<?>> settings = unmodifiableMap(new HashMap<String, Setting<?>>() {
        {
            /**
             * forecast enable/disable setting
             */
            put(FORECAST_ENABLED, Setting.boolSetting(FORECAST_ENABLED, true, NodeScope, Dynamic));

            /**
             * forecast breaker enable/disable setting
             */
            put(FORECAST_BREAKER_ENABLED, Setting.boolSetting(FORECAST_BREAKER_ENABLED, true, NodeScope, Dynamic));

            /**
             * We have a bloom filter placed in front of inactive entity cache to
             * filter out unpopular items that are not likely to appear more
             * than once. Whether this bloom filter is enabled or not.
             */
            put(
                FORECAST_DOOR_KEEPER_IN_CACHE_ENABLED,
                Setting.boolSetting(FORECAST_DOOR_KEEPER_IN_CACHE_ENABLED, false, NodeScope, Dynamic)
            );
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
        return ForecastEnabledSetting.getInstance().getSettingValue(ForecastEnabledSetting.FORECAST_ENABLED);
    }

    /**
     * Whether forecast circuit breaker is enabled or not.  If disabled, an open circuit breaker wouldn't cause an forecast job to be stopped.
     * @return whether forecast circuit breaker is enabled or not.
     */
    public static boolean isForecastBreakerEnabled() {
        return ForecastEnabledSetting.getInstance().getSettingValue(ForecastEnabledSetting.FORECAST_BREAKER_ENABLED);
    }

    /**
     * If enabled, we filter out unpopular items that are not likely to appear more than once
     * @return wWhether door keeper in cache is enabled or not.
     */
    public static boolean isDoorKeeperInCacheEnabled() {
        return ForecastEnabledSetting.getInstance().getSettingValue(ForecastEnabledSetting.FORECAST_DOOR_KEEPER_IN_CACHE_ENABLED);
    }
}
