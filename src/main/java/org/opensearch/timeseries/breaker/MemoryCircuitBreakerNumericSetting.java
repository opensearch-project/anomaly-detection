/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.breaker;

import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.common.settings.Setting;
import org.opensearch.timeseries.settings.DynamicNumericSetting;

public class MemoryCircuitBreakerNumericSetting extends DynamicNumericSetting {

    /**
     * Singleton instance
     */
    private static MemoryCircuitBreakerNumericSetting INSTANCE;

    /**
     * Setting name
     */
    public static final String JVM_HEAP_USAGE_THRESHOLD = "plugins.ad.jvm_heap_usage.threshold";

    public MemoryCircuitBreakerNumericSetting(Map<String, Setting<?>> settings) {
        super(settings);
    }

    public static synchronized MemoryCircuitBreakerNumericSetting getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new MemoryCircuitBreakerNumericSetting(settings);
        }
        return INSTANCE;
    }

    /**
     * Get the jvm_heap_usage threshold setting value
     * @return jvm_heap_usage threshold setting value
     */
    public static int getJVMHeapUsageThreshold() {
        return MemoryCircuitBreakerNumericSetting
            .getInstance()
            .getSettingValue(MemoryCircuitBreakerNumericSetting.JVM_HEAP_USAGE_THRESHOLD);
    }

    private static final Map<String, Setting<?>> settings = unmodifiableMap(new HashMap<String, Setting<?>>() {
        {
            put(
                JVM_HEAP_USAGE_THRESHOLD,
                Setting.intSetting(JVM_HEAP_USAGE_THRESHOLD, 95, 0, 98, Setting.Property.NodeScope, Setting.Property.Dynamic)
            );
        }
    });
}
