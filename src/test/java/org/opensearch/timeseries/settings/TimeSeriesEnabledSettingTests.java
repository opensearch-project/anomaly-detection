/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.settings;

import org.opensearch.test.OpenSearchTestCase;

public class TimeSeriesEnabledSettingTests extends OpenSearchTestCase {
    public void testIsForecastBreakerEnabled() {
        assertTrue(TimeSeriesEnabledSetting.isBreakerEnabled());
        TimeSeriesEnabledSetting.getInstance().setSettingValue(TimeSeriesEnabledSetting.BREAKER_ENABLED, false);
        assertTrue(!TimeSeriesEnabledSetting.isBreakerEnabled());
    }
}
