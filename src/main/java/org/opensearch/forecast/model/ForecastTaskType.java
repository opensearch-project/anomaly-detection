/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.forecast.model;

import java.util.List;

import org.opensearch.timeseries.model.TaskType;

import com.google.common.collect.ImmutableList;

/**
 * The ForecastTaskType enum defines different task types for forecasting, categorized into real-time and historical settings.
 * In real-time forecasting, we monitor states at the forecaster level, resulting in two distinct task types: one for
 * single-stream forecasting and another for high cardinality (HC). In the historical setting, state tracking is more nuanced,
 * encompassing both entity and forecaster levels. This leads to three specific task types: a forecaster-level task dedicated
 * to single-stream forecasting, and two tasks for HC, one at the forecaster level and another at the entity level.
 *
 * Real-time forecasting:
 * - REALTIME_FORECAST_SINGLE_STREAM: Represents a task type for single-stream forecasting. Ideal for scenarios where a single
 *  time series is processed in real-time.
 * - REALTIME_FORECAST_HC_FORECASTER: Represents a task type for high cardinality (HC) forecasting. Used when dealing with a
 *  large number of distinct entities in real-time.
 *
 * Run once forecasting:
 * - RUN_ONCE_FORECAST_SINGLE_STREAM: forecast once in single-stream scenario.
 * - RUN_ONCE_FORECAST_HC_FORECASTER: forecast once in HC scenario.
 *
 * enum names need to start with REALTIME or HISTORICAL we use prefix in TaskManager to check if a task is of certain type (e.g., historical)
 *
 */
public enum ForecastTaskType implements TaskType {
    REALTIME_FORECAST_SINGLE_STREAM,
    REALTIME_FORECAST_HC_FORECASTER,
    RUN_ONCE_FORECAST_SINGLE_STREAM,
    RUN_ONCE_FORECAST_HC_FORECASTER;

    public static List<ForecastTaskType> REALTIME_TASK_TYPES = ImmutableList
        .of(ForecastTaskType.REALTIME_FORECAST_SINGLE_STREAM, ForecastTaskType.REALTIME_FORECAST_HC_FORECASTER);
    public static List<ForecastTaskType> ALL_FORECAST_TASK_TYPES = ImmutableList
        .of(
            REALTIME_FORECAST_SINGLE_STREAM,
            REALTIME_FORECAST_HC_FORECASTER,
            RUN_ONCE_FORECAST_SINGLE_STREAM,
            RUN_ONCE_FORECAST_HC_FORECASTER
        );
    public static List<ForecastTaskType> RUN_ONCE_TASK_TYPES = ImmutableList
        .of(ForecastTaskType.RUN_ONCE_FORECAST_SINGLE_STREAM, ForecastTaskType.RUN_ONCE_FORECAST_HC_FORECASTER);
}
