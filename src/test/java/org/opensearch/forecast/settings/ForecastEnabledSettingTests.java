/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.settings;

import org.opensearch.test.OpenSearchTestCase;

public class ForecastEnabledSettingTests extends OpenSearchTestCase {

    public void testIsForecastEnabled() {
        boolean original = ForecastEnabledSetting.isForecastEnabled();
        try {
            ForecastEnabledSetting.getInstance().setSettingValue(ForecastEnabledSetting.FORECAST_ENABLED, true);
            // TODO: currently, we disable forecasting
            assertTrue(ForecastEnabledSetting.isForecastEnabled());
            // assertTrue(!ForecastEnabledSetting.isForecastEnabled());
            ForecastEnabledSetting.getInstance().setSettingValue(ForecastEnabledSetting.FORECAST_ENABLED, false);
            assertTrue(!ForecastEnabledSetting.isForecastEnabled());
        } finally {
            ForecastEnabledSetting.getInstance().setSettingValue(ForecastEnabledSetting.FORECAST_ENABLED, original);
        }
    }

}
