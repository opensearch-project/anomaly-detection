/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.settings;

import static org.mockito.Mockito.mock;
import static org.opensearch.common.settings.Setting.Property.Dynamic;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.Mockito;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

public class ADEnabledSettingTests extends OpenSearchTestCase {

    public void testIsADEnabled() {
        assertTrue(ADEnabledSetting.isADEnabled());
        ADEnabledSetting.getInstance().setSettingValue(ADEnabledSetting.AD_ENABLED, false);
        assertTrue(!ADEnabledSetting.isADEnabled());
        ADEnabledSetting.getInstance().setSettingValue(ADEnabledSetting.AD_ENABLED, true);
    }

    public void testIsADBreakerEnabled() {
        assertTrue(ADEnabledSetting.isADBreakerEnabled());
        ADEnabledSetting.getInstance().setSettingValue(ADEnabledSetting.AD_BREAKER_ENABLED, false);
        assertTrue(!ADEnabledSetting.isADBreakerEnabled());
    }

    public void testIsInterpolationInColdStartEnabled() {
        assertTrue(!ADEnabledSetting.isInterpolationInColdStartEnabled());
        ADEnabledSetting.getInstance().setSettingValue(ADEnabledSetting.INTERPOLATION_IN_HCAD_COLD_START_ENABLED, true);
        assertTrue(ADEnabledSetting.isInterpolationInColdStartEnabled());
    }

    public void testIsDoorKeeperInCacheEnabled() {
        assertTrue(!ADEnabledSetting.isDoorKeeperInCacheEnabled());
        ADEnabledSetting.getInstance().setSettingValue(ADEnabledSetting.DOOR_KEEPER_IN_CACHE_ENABLED, true);
        assertTrue(ADEnabledSetting.isDoorKeeperInCacheEnabled());
    }

    public void testSetSettingsUpdateConsumers() {
        Setting<Boolean> testSetting = Setting.boolSetting("test.setting", true, Setting.Property.NodeScope, Dynamic);
        Map<String, Setting<?>> settings = new HashMap<>();
        settings.put("test.setting", testSetting);
        ADEnabledSetting dynamicNumericSetting = new ADEnabledSetting(settings);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Collections.singleton(testSetting));
        ClusterService clusterService = mock(ClusterService.class);
        Mockito.when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        dynamicNumericSetting.init(clusterService);

        assertEquals(true, dynamicNumericSetting.getSettingValue("test.setting"));
    }

    public void testGetSettings() {
        Setting<Boolean> testSetting1 = Setting.boolSetting("test.setting1", true, Setting.Property.NodeScope);
        Setting<Boolean> testSetting2 = Setting.boolSetting("test.setting2", false, Setting.Property.NodeScope);
        Map<String, Setting<?>> settings = new HashMap<>();
        settings.put("test.setting1", testSetting1);
        settings.put("test.setting2", testSetting2);
        ADEnabledSetting dynamicNumericSetting = new ADEnabledSetting(settings);
        List<Setting<?>> returnedSettings = dynamicNumericSetting.getSettings();
        assertEquals(2, returnedSettings.size());
        assertTrue(returnedSettings.containsAll(settings.values()));
    }
}
