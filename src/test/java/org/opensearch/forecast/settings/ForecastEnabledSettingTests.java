/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.settings;

import org.opensearch.test.OpenSearchTestCase;

public class ForecastEnabledSettingTests extends OpenSearchTestCase {

    public void testIsForecastEnabled() {
        assertTrue(ForecastEnabledSetting.isForecastEnabled());
        ForecastEnabledSetting.getInstance().setSettingValue(ForecastEnabledSetting.FORECAST_ENABLED, false);
        assertTrue(!ForecastEnabledSetting.isForecastEnabled());
    }

    public void testIsForecastBreakerEnabled() {
        assertTrue(ForecastEnabledSetting.isForecastBreakerEnabled());
        ForecastEnabledSetting.getInstance().setSettingValue(ForecastEnabledSetting.FORECAST_BREAKER_ENABLED, false);
        assertTrue(!ForecastEnabledSetting.isForecastBreakerEnabled());
    }

    public void testIsDoorKeeperInCacheEnabled() {
        assertTrue(!ForecastEnabledSetting.isDoorKeeperInCacheEnabled());
        ForecastEnabledSetting.getInstance().setSettingValue(ForecastEnabledSetting.FORECAST_DOOR_KEEPER_IN_CACHE_ENABLED, true);
        assertTrue(ForecastEnabledSetting.isDoorKeeperInCacheEnabled());
    }

}
