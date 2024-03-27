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
 * - FORECAST_REALTIME_SINGLE_STREAM: Represents a task type for single-stream forecasting. Ideal for scenarios where a single
 *  time series is processed in real-time.
 * - FORECAST_REALTIME_HC_FORECASTER: Represents a task type for high cardinality (HC) forecasting. Used when dealing with a
 *  large number of distinct entities in real-time.
 *
 * Historical forecasting:
 * - FORECAST_HISTORICAL_SINGLE_STREAM: Represents a forecaster-level task for single-stream historical forecasting.
 * Suitable for analyzing a single time series in a sequential manner.
 * - FORECAST_HISTORICAL_HC_FORECASTER: A forecaster-level task to track overall state, initialization progress, errors, etc.,
 * for HC forecasting. Central to managing multiple historical time series with high cardinality.
 * - FORECAST_HISTORICAL_HC_ENTITY: An entity-level task to track the state, initialization progress, errors, etc., of a
 * specific entity within HC historical forecasting. Allows for fine-grained information recording at the entity level.
 *
 */
public enum ForecastTaskType implements TaskType {
    FORECAST_REALTIME_SINGLE_STREAM,
    FORECAST_REALTIME_HC_FORECASTER,
    FORECAST_HISTORICAL_SINGLE_STREAM,
    // forecaster level task to track overall state, init progress, error etc. for HC forecaster
    FORECAST_HISTORICAL_HC_FORECASTER,
    // entity level task to track just one specific entity's state, init progress, error etc.
    FORECAST_HISTORICAL_HC_ENTITY;

    public static List<ForecastTaskType> HISTORICAL_FORECASTER_TASK_TYPES = ImmutableList
        .of(ForecastTaskType.FORECAST_HISTORICAL_HC_FORECASTER, ForecastTaskType.FORECAST_HISTORICAL_SINGLE_STREAM);
    public static List<ForecastTaskType> ALL_HISTORICAL_TASK_TYPES = ImmutableList
        .of(
            ForecastTaskType.FORECAST_HISTORICAL_HC_FORECASTER,
            ForecastTaskType.FORECAST_HISTORICAL_SINGLE_STREAM,
            ForecastTaskType.FORECAST_HISTORICAL_HC_ENTITY
        );
    public static List<ForecastTaskType> REALTIME_TASK_TYPES = ImmutableList
        .of(ForecastTaskType.FORECAST_REALTIME_SINGLE_STREAM, ForecastTaskType.FORECAST_REALTIME_HC_FORECASTER);
    public static List<ForecastTaskType> ALL_FORECAST_TASK_TYPES = ImmutableList
        .of(
            ForecastTaskType.FORECAST_REALTIME_SINGLE_STREAM,
            ForecastTaskType.FORECAST_REALTIME_HC_FORECASTER,
            ForecastTaskType.FORECAST_HISTORICAL_SINGLE_STREAM,
            ForecastTaskType.FORECAST_HISTORICAL_HC_FORECASTER,
            ForecastTaskType.FORECAST_HISTORICAL_HC_ENTITY
        );
}
