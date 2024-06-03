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

package org.opensearch.forecast.rest;

import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.transport.SearchForecastTasksAction;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to search AD tasks.
 */
public class RestSearchForecastTasksAction extends AbstractForecastSearchAction<ForecastTask> {

    private static final String URL_PATH = TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI + "/tasks/_search";
    private final String SEARCH_FORECASTER_TASKS = "search_forecaster_tasks";

    public RestSearchForecastTasksAction() {
        super(
            ImmutableList.of(URL_PATH),
            ImmutableList.of(),
            ForecastIndex.STATE.getIndexName(),
            ForecastTask.class,
            SearchForecastTasksAction.INSTANCE
        );
    }

    @Override
    public String getName() {
        return SEARCH_FORECASTER_TASKS;
    }

}
