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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.settings;

import static java.util.Collections.unmodifiableMap;
import static org.opensearch.common.settings.Setting.Property.Deprecated;
import static org.opensearch.common.settings.Setting.Property.Dynamic;
import static org.opensearch.common.settings.Setting.Property.NodeScope;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

public class EnabledSetting {

    private static Logger logger = LogManager.getLogger(EnabledSetting.class);

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

    private final Map<String, Setting<?>> settings = unmodifiableMap(new HashMap<String, Setting<?>>() {
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
        }
    });

    /** Latest setting value for each registered key. Thread-safe is required. */
    private final Map<String, Object> latestSettings = new ConcurrentHashMap<>();

    private ClusterService clusterService;

    private EnabledSetting() {}

    public static synchronized EnabledSetting getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new EnabledSetting();
        }
        return INSTANCE;
    }

    private void setSettingsUpdateConsumers() {
        for (Setting<?> setting : settings.values()) {
            clusterService.getClusterSettings().addSettingsUpdateConsumer(setting, newVal -> {
                logger.info("[AD] The value of setting [{}] changed to [{}]", setting.getKey(), newVal);
                latestSettings.put(setting.getKey(), newVal);
            });
        }
    }

    /**
     * Get setting value by key. Return default value if not configured explicitly.
     *
     * @param key   setting key.
     * @param <T> Setting type
     * @return T     setting value or default
     */
    @SuppressWarnings("unchecked")
    public <T> T getSettingValue(String key) {
        return (T) latestSettings.getOrDefault(key, getSetting(key).getDefault(Settings.EMPTY));
    }

    private Setting<?> getSetting(String key) {
        if (settings.containsKey(key)) {
            return settings.get(key);
        }
        throw new IllegalArgumentException("Cannot find setting by key [" + key + "]");
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

    public void init(ClusterService clusterService) {
        this.clusterService = clusterService;
        setSettingsUpdateConsumers();
    }

    public List<Setting<?>> getSettings() {
        return new ArrayList<>(settings.values());
    }
}
