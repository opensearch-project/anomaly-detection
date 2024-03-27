/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.settings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

/**
 * A container serving dynamic numeric setting.  The caller does not have to call
 * ClusterSettings.addSettingsUpdateConsumer and can access the most-up-to-date
 * value using the singleton instance. This is convenient for a setting that's
 * accessed by various places or it is not possible to install ClusterSettings.addSettingsUpdateConsumer
 * as the enclosing instances are not singleton (i.e. deleted after use).
 *
 */
public abstract class DynamicNumericSetting {
    private static Logger logger = LogManager.getLogger(DynamicNumericSetting.class);

    private ClusterService clusterService;
    /** Latest setting value for each registered key. Thread-safe is required. */
    private final Map<String, Object> latestSettings = new ConcurrentHashMap<>();

    private final Map<String, Setting<?>> settings;

    protected DynamicNumericSetting(Map<String, Setting<?>> settings) {
        this.settings = settings;
    }

    private void setSettingsUpdateConsumers() {
        for (Setting<?> setting : settings.values()) {
            clusterService.getClusterSettings().addSettingsUpdateConsumer(setting, newVal -> {
                logger.info("[AD] The value of setting [{}] changed to [{}]", setting.getKey(), newVal);
                latestSettings.put(setting.getKey(), newVal);
            });
        }
    }

    public void init(ClusterService clusterService) {
        this.clusterService = clusterService;
        setSettingsUpdateConsumers();
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

    /**
     * Override existing value.
     * @param key Key
     * @param newVal New value
     */
    public void setSettingValue(String key, Object newVal) {
        latestSettings.put(key, newVal);
    }

    private Setting<?> getSetting(String key) {
        if (settings.containsKey(key)) {
            return settings.get(key);
        }
        throw new IllegalArgumentException("Cannot find setting by key [" + key + "]");
    }

    public List<Setting<?>> getSettings() {
        return new ArrayList<>(settings.values());
    }
}
