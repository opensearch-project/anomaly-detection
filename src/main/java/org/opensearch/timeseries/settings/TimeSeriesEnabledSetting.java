/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.settings;

import static java.util.Collections.unmodifiableMap;
import static org.opensearch.common.settings.Setting.Property.Dynamic;
import static org.opensearch.common.settings.Setting.Property.NodeScope;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.common.settings.Setting;

public class TimeSeriesEnabledSetting extends DynamicNumericSetting {

    /**
     * Singleton instance
     */
    private static TimeSeriesEnabledSetting INSTANCE;

    /**
     * Settings name
     */
    public static final String BREAKER_ENABLED = "plugins.timeseries.breaker.enabled";

    public static final Map<String, Setting<?>> settings = unmodifiableMap(new HashMap<String, Setting<?>>() {
        {
            /**
             * breaker enable/disable setting. Default value comes from AD breaker enabled or not for BWC.
             */
            put(BREAKER_ENABLED, Setting.boolSetting(BREAKER_ENABLED, ADEnabledSetting.isADBreakerEnabled(), NodeScope, Dynamic));
        }
    });

    private TimeSeriesEnabledSetting(Map<String, Setting<?>> settings) {
        super(settings);
    }

    public static synchronized TimeSeriesEnabledSetting getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new TimeSeriesEnabledSetting(settings);
        }
        return INSTANCE;
    }

    /**
     * Whether circuit breaker is enabled or not.  If disabled, an open circuit breaker wouldn't cause a real-time job to be stopped.
     * @return whether circuit breaker is enabled or not.
     */
    public static boolean isBreakerEnabled() {
        return TimeSeriesEnabledSetting.getInstance().getSettingValue(TimeSeriesEnabledSetting.BREAKER_ENABLED);
    }

}
