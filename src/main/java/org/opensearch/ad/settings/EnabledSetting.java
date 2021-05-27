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

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;

public class EnabledSetting extends AbstractSetting {

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

    private static final Map<String, Setting<?>> settings = unmodifiableMap(new HashMap<String, Setting<?>>() {
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
}
