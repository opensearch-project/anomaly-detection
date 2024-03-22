/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.model;

import java.util.Arrays;

import org.opensearch.test.OpenSearchTestCase;

public class ForecastTaskTypeTests extends OpenSearchTestCase {

    public void testHistoricalForecasterTaskTypes() {
        assertEquals(
            Arrays.asList(ForecastTaskType.FORECAST_HISTORICAL_HC_FORECASTER, ForecastTaskType.FORECAST_HISTORICAL_SINGLE_STREAM),
            ForecastTaskType.HISTORICAL_FORECASTER_TASK_TYPES
        );
    }

    public void testAllHistoricalTaskTypes() {
        assertEquals(
            Arrays
                .asList(
                    ForecastTaskType.FORECAST_HISTORICAL_HC_FORECASTER,
                    ForecastTaskType.FORECAST_HISTORICAL_SINGLE_STREAM,
                    ForecastTaskType.FORECAST_HISTORICAL_HC_ENTITY
                ),
            ForecastTaskType.ALL_HISTORICAL_TASK_TYPES
        );
    }

    public void testRealtimeTaskTypes() {
        assertEquals(
            Arrays.asList(ForecastTaskType.FORECAST_REALTIME_SINGLE_STREAM, ForecastTaskType.FORECAST_REALTIME_HC_FORECASTER),
            ForecastTaskType.REALTIME_TASK_TYPES
        );
    }

    public void testAllForecastTaskTypes() {
        assertEquals(
            Arrays
                .asList(
                    ForecastTaskType.FORECAST_REALTIME_SINGLE_STREAM,
                    ForecastTaskType.FORECAST_REALTIME_HC_FORECASTER,
                    ForecastTaskType.FORECAST_HISTORICAL_SINGLE_STREAM,
                    ForecastTaskType.FORECAST_HISTORICAL_HC_FORECASTER,
                    ForecastTaskType.FORECAST_HISTORICAL_HC_ENTITY
                ),
            ForecastTaskType.ALL_FORECAST_TASK_TYPES
        );
    }
}
