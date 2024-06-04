/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.settings;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.opensearch.common.settings.Setting;
import org.opensearch.test.OpenSearchTestCase;

public class ADNumericSettingTests extends OpenSearchTestCase {
    private ADNumericSetting adSetting;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        adSetting = ADNumericSetting.getInstance();
    }

    public void testMaxCategoricalFields() {
        adSetting.setSettingValue(ADNumericSetting.CATEGORY_FIELD_LIMIT, 3);
        int value = ADNumericSetting.maxCategoricalFields();
        assertEquals("Expected value is 3", 3, value);
    }

    public void testGetThresholdValue_shouldReturnThresholdValue() {
        adSetting.setSettingValue(ADNumericSetting.JVM_HEAP_USAGE_THRESHOLD, 96);
        int value = ADNumericSetting.getJVMHeapUsageThreshold();
        assertEquals(96, value);
    }

    public void testGetSettingValue() {
        Map<String, Setting<?>> settingsMap = new HashMap<>();
        Setting<Integer> testSetting = Setting.intSetting("test.setting", 1, Setting.Property.NodeScope);
        settingsMap.put("test.setting", testSetting);
        adSetting = new ADNumericSetting(settingsMap);

        adSetting.setSettingValue("test.setting", 2);
        Integer value = adSetting.getSettingValue("test.setting");
        assertEquals("Expected value is 2", 2, value.intValue());
    }

    public void testGetSettingNonexistentKey() {
        try {
            adSetting.getSettingValue("nonexistent.key");
            fail("Expected an IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            assertEquals("Cannot find setting by key [nonexistent.key]", e.getMessage());
        }
    }
}
