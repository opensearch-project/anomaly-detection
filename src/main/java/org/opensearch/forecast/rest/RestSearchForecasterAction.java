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

import static org.opensearch.timeseries.util.RestHandlerUtils.SEARCH;

import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.transport.SearchForecasterAction;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to search anomaly detectors.
 */
public class RestSearchForecasterAction extends AbstractForecastSearchAction<Forecaster> {

    private static final String URL_PATH = TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI + "/" + SEARCH;
    private final String SEARCH_FORECASTER_ACTION = "search_forecaster";

    public RestSearchForecasterAction() {
        super(
            ImmutableList.of(URL_PATH),
            ImmutableList.of(),
            ForecastCommonName.CONFIG_INDEX,
            Forecaster.class,
            SearchForecasterAction.INSTANCE
        );
    }

    @Override
    public String getName() {
        return SEARCH_FORECASTER_ACTION;
    }
}
