/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.settings;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.opensearch.common.settings.Setting;
import org.opensearch.test.OpenSearchTestCase;

public class ForecastNumericSettingTests extends OpenSearchTestCase {
    private ForecastNumericSetting forecastSetting;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        forecastSetting = ForecastNumericSetting.getInstance();
    }

    public void testMaxCategoricalFields() {
        forecastSetting.setSettingValue(ForecastNumericSetting.CATEGORY_FIELD_LIMIT, 3);
        int value = ForecastNumericSetting.maxCategoricalFields();
        assertEquals("Expected value is 3", 3, value);
    }

    public void testGetSettingValue() {
        Map<String, Setting<?>> settingsMap = new HashMap<>();
        Setting<Integer> testSetting = Setting.intSetting("test.setting", 1, Setting.Property.NodeScope);
        settingsMap.put("test.setting", testSetting);
        forecastSetting = new ForecastNumericSetting(settingsMap);

        forecastSetting.setSettingValue("test.setting", 2);
        Integer value = forecastSetting.getSettingValue("test.setting");
        assertEquals("Expected value is 2", 2, value.intValue());
    }

    public void testGetSettingNonexistentKey() {
        try {
            forecastSetting.getSettingValue("nonexistent.key");
            fail("Expected an IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            assertEquals("Cannot find setting by key [nonexistent.key]", e.getMessage());
        }
    }
}
