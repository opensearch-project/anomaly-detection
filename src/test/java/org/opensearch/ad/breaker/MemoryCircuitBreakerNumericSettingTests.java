/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.breaker;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.common.settings.Setting;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.breaker.MemoryCircuitBreakerNumericSetting;

public class MemoryCircuitBreakerNumericSettingTests extends OpenSearchTestCase {
    private MemoryCircuitBreakerNumericSetting memoryCircuitBreakerNumericSetting;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        memoryCircuitBreakerNumericSetting = MemoryCircuitBreakerNumericSetting.getInstance();
    }

    @Test
    public void testGetThresholdValue_shouldReturnThresholdValue() {
        memoryCircuitBreakerNumericSetting.setSettingValue(MemoryCircuitBreakerNumericSetting.JVM_HEAP_USAGE_THRESHOLD, 96);
        int value = MemoryCircuitBreakerNumericSetting.getJVMHeapUsageThreshold();
        assertEquals(96, value);
    }

    @Test
    public void testGetSettingValue_shouldReturnSettingValue() {
        Map<String, Setting<?>> settingsMap = new HashMap<>();
        Setting<Integer> testSetting = Setting.intSetting("test.setting", 1, Setting.Property.NodeScope);
        settingsMap.put("test.setting", testSetting);
        memoryCircuitBreakerNumericSetting = new MemoryCircuitBreakerNumericSetting(settingsMap);

        memoryCircuitBreakerNumericSetting.setSettingValue("test.setting", 2);
        Integer value = memoryCircuitBreakerNumericSetting.getSettingValue("test.setting");
        assertEquals(2, value.intValue());
    }

    @Test
    public void testGetSettingValue_withNonexistent_shouldThrowException() {
        try {
            memoryCircuitBreakerNumericSetting.getSettingValue("nonexistent.key");
            fail("Expected an IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            assertEquals("Cannot find setting by key [nonexistent.key]", e.getMessage());
        }
    }
}
