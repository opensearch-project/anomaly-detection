/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.model;

import java.util.Arrays;

import org.opensearch.test.OpenSearchTestCase;

public class ForecastTaskTypeTests extends OpenSearchTestCase {

    public void testRunOnceForecasterTaskTypes() {
        assertEquals(
            Arrays.asList(ForecastTaskType.RUN_ONCE_FORECAST_SINGLE_STREAM, ForecastTaskType.RUN_ONCE_FORECAST_HC_FORECASTER),
            ForecastTaskType.RUN_ONCE_TASK_TYPES
        );
    }

    public void testRealtimeTaskTypes() {
        assertEquals(
            Arrays.asList(ForecastTaskType.REALTIME_FORECAST_SINGLE_STREAM, ForecastTaskType.REALTIME_FORECAST_HC_FORECASTER),
            ForecastTaskType.REALTIME_TASK_TYPES
        );
    }

    public void testAllForecastTaskTypes() {
        assertEquals(
            Arrays
                .asList(
                    ForecastTaskType.REALTIME_FORECAST_SINGLE_STREAM,
                    ForecastTaskType.REALTIME_FORECAST_HC_FORECASTER,
                    ForecastTaskType.RUN_ONCE_FORECAST_SINGLE_STREAM,
                    ForecastTaskType.RUN_ONCE_FORECAST_HC_FORECASTER
                ),
            ForecastTaskType.ALL_FORECAST_TASK_TYPES
        );
    }
}
