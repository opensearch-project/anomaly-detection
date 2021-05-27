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
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
public abstract class AbstractSetting {
    private static Logger logger = LogManager.getLogger(AbstractSetting.class);

    private ClusterService clusterService;
    /** Latest setting value for each registered key. Thread-safe is required. */
    private final Map<String, Object> latestSettings = new ConcurrentHashMap<>();

    private final Map<String, Setting<?>> settings;

    protected AbstractSetting(Map<String, Setting<?>> settings) {
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
