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

package org.opensearch.ad.settings;

import static java.util.Collections.unmodifiableMap;
import static org.opensearch.common.settings.Setting.Property.Deprecated;
import static org.opensearch.common.settings.Setting.Property.Dynamic;
import static org.opensearch.common.settings.Setting.Property.NodeScope;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.common.settings.Setting;

public class EnabledSetting extends AbstractSetting {

    /**
     * Singleton instance
     */
    private static EnabledSetting INSTANCE;

    /**
     * Settings name
     */
    public static final String AD_PLUGIN_ENABLED = "plugins.anomaly_detection.enabled";

    public static final String AD_BREAKER_ENABLED = "plugins.anomaly_detection.breaker.enabled";

    public static final String LEGACY_OPENDISTRO_AD_PLUGIN_ENABLED = "opendistro.anomaly_detection.enabled";

    public static final String LEGACY_OPENDISTRO_AD_BREAKER_ENABLED = "opendistro.anomaly_detection.breaker.enabled";

    public static final String INTERPOLATION_IN_HCAD_COLD_START_ENABLED = "plugins.anomaly_detection.hcad_cold_start_interpolation.enabled";

    public static final String DOOR_KEEPER_IN_CACHE_ENABLED = "plugins.anomaly_detection.door_keeper_in_cache.enabled";;

    public static final Map<String, Setting<?>> settings = unmodifiableMap(new HashMap<String, Setting<?>>() {
        {
            Setting LegacyADPluginEnabledSetting = Setting
                .boolSetting(LEGACY_OPENDISTRO_AD_PLUGIN_ENABLED, true, NodeScope, Dynamic, Deprecated);
            /**
             * Legacy OpenDistro AD plugin enable/disable setting
             */
            put(LEGACY_OPENDISTRO_AD_PLUGIN_ENABLED, LegacyADPluginEnabledSetting);

            Setting LegacyADBreakerEnabledSetting = Setting
                .boolSetting(LEGACY_OPENDISTRO_AD_BREAKER_ENABLED, true, NodeScope, Dynamic, Deprecated);
            /**
             * Legacy OpenDistro AD breaker enable/disable setting
             */
            put(LEGACY_OPENDISTRO_AD_BREAKER_ENABLED, LegacyADBreakerEnabledSetting);

            /**
             * AD plugin enable/disable setting
             */
            put(AD_PLUGIN_ENABLED, Setting.boolSetting(AD_PLUGIN_ENABLED, LegacyADPluginEnabledSetting, NodeScope, Dynamic));

            /**
             * AD breaker enable/disable setting
             */
            put(AD_BREAKER_ENABLED, Setting.boolSetting(AD_BREAKER_ENABLED, LegacyADBreakerEnabledSetting, NodeScope, Dynamic));

            /**
             * Whether interpolation in HCAD cold start is enabled or not
             */
            put(
                INTERPOLATION_IN_HCAD_COLD_START_ENABLED,
                Setting.boolSetting(INTERPOLATION_IN_HCAD_COLD_START_ENABLED, false, NodeScope, Dynamic)
            );

            /**
             * We have a bloom filter placed in front of inactive entity cache to
             * filter out unpopular items that are not likely to appear more
             * than once. Whether this bloom filter is enabled or not.
             */
            put(DOOR_KEEPER_IN_CACHE_ENABLED, Setting.boolSetting(DOOR_KEEPER_IN_CACHE_ENABLED, false, NodeScope, Dynamic));
        }
    });

    private EnabledSetting(Map<String, Setting<?>> settings) {
        super(settings);
    }

    public static synchronized EnabledSetting getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new EnabledSetting(settings);
        }
        return INSTANCE;
    }

    /**
     * Whether AD plugin is enabled.  If disabled, AD plugin rejects RESTful requests and stop all AD jobs.
     * @return whether AD plugin is enabled.
     */
    public static boolean isADPluginEnabled() {
        return EnabledSetting.getInstance().getSettingValue(EnabledSetting.AD_PLUGIN_ENABLED);
    }

    /**
     * Whether AD circuit breaker is enabled or not.  If disabled, an open circuit breaker wouldn't cause an AD job to be stopped.
     * @return whether AD circuit breaker is enabled or not.
     */
    public static boolean isADBreakerEnabled() {
        return EnabledSetting.getInstance().getSettingValue(EnabledSetting.AD_BREAKER_ENABLED);
    }

    /**
     * If enabled, we use samples plus interpolation to train models.
     * @return wWhether interpolation in HCAD cold start is enabled or not.
     */
    public static boolean isInterpolationInColdStartEnabled() {
        return EnabledSetting.getInstance().getSettingValue(EnabledSetting.INTERPOLATION_IN_HCAD_COLD_START_ENABLED);
    }

    /**
     * If enabled, we filter out unpopular items that are not likely to appear more than once
     * @return wWhether door keeper in cache is enabled or not.
     */
    public static boolean isDoorKeeperInCacheEnabled() {
        return EnabledSetting.getInstance().getSettingValue(EnabledSetting.DOOR_KEEPER_IN_CACHE_ENABLED);
    }
}
