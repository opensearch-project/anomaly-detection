/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import static org.opensearch.ad.model.ADTaskType.ALL_HISTORICAL_TASK_TYPES;
import static org.opensearch.ad.model.ADTaskType.HISTORICAL_DETECTOR_TASK_TYPES;
import static org.opensearch.ad.model.ADTaskType.REALTIME_TASK_TYPES;

import java.util.List;

import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.TaskType;

public class TaskUtil {
    public static List<? extends TaskType> getTaskTypes(DateRange dateRange, boolean resetLatestTaskStateFlag, AnalysisType analysisType) {
        if (analysisType == AnalysisType.FORECAST) {
            if (dateRange == null) {
                return ForecastTaskType.REALTIME_TASK_TYPES;
            } else {
                throw new UnsupportedOperationException("Forecasting does not support historical tasks");
            }
        } else {
            if (dateRange == null) {
                return REALTIME_TASK_TYPES;
            } else {
                if (resetLatestTaskStateFlag) {
                    // return all task types include HC entity task to make sure we can reset all tasks latest flag
                    return ALL_HISTORICAL_TASK_TYPES;
                } else {
                    return HISTORICAL_DETECTOR_TASK_TYPES;
                }
            }
        }

    }
}
