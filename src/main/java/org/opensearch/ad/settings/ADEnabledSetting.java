/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.settings;

import static java.util.Collections.unmodifiableMap;
import static org.opensearch.common.settings.Setting.Property.Deprecated;
import static org.opensearch.common.settings.Setting.Property.Dynamic;
import static org.opensearch.common.settings.Setting.Property.NodeScope;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.common.settings.Setting;
import org.opensearch.timeseries.settings.DynamicNumericSetting;

public class ADEnabledSetting extends DynamicNumericSetting {

    /**
     * Singleton instance
     */
    private static ADEnabledSetting INSTANCE;

    /**
     * Settings name
     */
    public static final String AD_ENABLED = "plugins.anomaly_detection.enabled";

    // use TimeSeriesEnabledSetting.BREAKER_ENABLED instead
    @Deprecated
    public static final String AD_BREAKER_ENABLED = "plugins.anomaly_detection.breaker.enabled";

    public static final String LEGACY_OPENDISTRO_AD_ENABLED = "opendistro.anomaly_detection.enabled";

    public static final String LEGACY_OPENDISTRO_AD_BREAKER_ENABLED = "opendistro.anomaly_detection.breaker.enabled";

    // we don't support interpolation during cold start starting 3.0 (TODO replace with the right version)
    @Deprecated
    public static final String INTERPOLATION_IN_HCAD_COLD_START_ENABLED = "plugins.anomaly_detection.hcad_cold_start_interpolation.enabled";

    public static final String DOOR_KEEPER_IN_CACHE_ENABLED = "plugins.anomaly_detection.door_keeper_in_cache.enabled";

    /**
     * Indicates whether multi-tenancy is enabled in Anomaly Detection.
     *
     * This is a static setting that must be configured before starting OpenSearch. The corresponding setting {@code plugins.ml_commons.multi_tenancy_enabled} in the ML Commons plugin should match.
     *
     * It can be set in the following ways, in priority order:
     *
     * <ol>
     *   <li>As a command-line argument using the <code>-E</code> flag (this overrides other options):
     *       <pre>
     *       ./bin/opensearch -Eplugins.anomaly_detection.multi_tenancy_enabled=true
     *       </pre>
     *   </li>
     *   <li>As a system property using <code>OPENSEARCH_JAVA_OPTS</code> (this overrides <code>opensearch.yml</code>):
     *       <pre>
     *       export OPENSEARCH_JAVA_OPTS="-Dplugins.anomaly_detection.multi_tenancy_enabled=true"
     *       ./bin/opensearch
     *       </pre>
     *       Or inline when starting OpenSearch:
     *       <pre>
     *       OPENSEARCH_JAVA_OPTS="-Dplugins.anomaly_detection.multi_tenancy_enabled=true" ./bin/opensearch
     *       </pre>
     *   </li>
     *   <li>In the <code>opensearch.yml</code> configuration file:
     *       <pre>
     *       plugins.anomaly_detection.multi_tenancy_enabled: true
     *       </pre>
     *   </li>
     * </ol>
     *
     * After setting this option, a full cluster restart is required for the changes to take effect.
     */
    public static final String AD_MULTI_TENANCY_ENABLED = "plugins.anomaly_detection.multi_tenancy_enabled";

    public static final Map<String, Setting<?>> settings = unmodifiableMap(new HashMap<String, Setting<?>>() {
        {
            Setting LegacyADEnabledSetting = Setting.boolSetting(LEGACY_OPENDISTRO_AD_ENABLED, true, NodeScope, Dynamic, Deprecated);
            /**
             * Legacy OpenDistro AD enable/disable setting
             */
            put(LEGACY_OPENDISTRO_AD_ENABLED, LegacyADEnabledSetting);

            Setting LegacyADBreakerEnabledSetting = Setting
                .boolSetting(LEGACY_OPENDISTRO_AD_BREAKER_ENABLED, true, NodeScope, Dynamic, Deprecated);
            /**
             * Legacy OpenDistro AD breaker enable/disable setting
             */
            put(LEGACY_OPENDISTRO_AD_BREAKER_ENABLED, LegacyADBreakerEnabledSetting);

            /**
             * AD enable/disable setting
             */
            put(AD_ENABLED, Setting.boolSetting(AD_ENABLED, LegacyADEnabledSetting, NodeScope, Dynamic));

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
             *
             * disable the door keeper by default so that
             * 1) we won't have to wait an extra interval before HCAD cold start.
             * 2) long interval detector (> 1hr) will have their model cleared in cache due to ttl. Door keeper will
             * keep those detectors from getting results.
             */
            put(DOOR_KEEPER_IN_CACHE_ENABLED, Setting.boolSetting(DOOR_KEEPER_IN_CACHE_ENABLED, false, NodeScope, Dynamic));
        }
    });

    ADEnabledSetting(Map<String, Setting<?>> settings) {
        super(settings);
    }

    public static synchronized ADEnabledSetting getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ADEnabledSetting(settings);
        }
        return INSTANCE;
    }

    /**
     * Whether AD is enabled.  If disabled, time series plugin rejects RESTful requests on AD and stop all AD jobs.
     * @return whether AD is enabled.
     */
    public static boolean isADEnabled() {
        return ADEnabledSetting.getInstance().getSettingValue(ADEnabledSetting.AD_ENABLED);
    }

    /**
     * Whether AD circuit breaker is enabled or not.  If disabled, an open circuit breaker wouldn't cause an AD job to be stopped.
     * @return whether AD circuit breaker is enabled or not.
     */
    public static boolean isADBreakerEnabled() {
        return ADEnabledSetting.getInstance().getSettingValue(ADEnabledSetting.AD_BREAKER_ENABLED);
    }

    /**
     * If enabled, we use samples plus interpolation to train models.
     * @return wWhether interpolation in HCAD cold start is enabled or not.
     */
    public static boolean isInterpolationInColdStartEnabled() {
        return ADEnabledSetting.getInstance().getSettingValue(ADEnabledSetting.INTERPOLATION_IN_HCAD_COLD_START_ENABLED);
    }

    /**
     * If enabled, we filter out unpopular items that are not likely to appear more than once
     * @return wWhether door keeper in cache is enabled or not.
     */
    public static boolean isDoorKeeperInCacheEnabled() {
        return ADEnabledSetting.getInstance().getSettingValue(ADEnabledSetting.DOOR_KEEPER_IN_CACHE_ENABLED);
    }

    /**
     * Whether AD multi-tenancy is enabled or not.
     * @return whether AD multi-tenancy is enabled.
     */
    public static boolean isADMultiTenancyEnabled() {
        return ADEnabledSetting.getInstance().getSettingValue(ADEnabledSetting.AD_MULTI_TENANCY_ENABLED);
    }
}
