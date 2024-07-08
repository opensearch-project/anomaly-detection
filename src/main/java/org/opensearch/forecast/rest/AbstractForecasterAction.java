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

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_INTERVAL;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_REQUEST_TIMEOUT;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_WINDOW_DELAY;
import static org.opensearch.forecast.settings.ForecastSettings.MAX_FORECAST_FEATURES;
import static org.opensearch.forecast.settings.ForecastSettings.MAX_HC_FORECASTERS;
import static org.opensearch.forecast.settings.ForecastSettings.MAX_SINGLE_STREAM_FORECASTERS;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.forecast.settings.ForecastNumericSetting;
import org.opensearch.rest.BaseRestHandler;

/**
 * This class consists of the base class for validating and indexing forecast REST handlers.
 */
public abstract class AbstractForecasterAction extends BaseRestHandler {
    /**
     * Timeout duration for the forecast request.
     */
    protected volatile TimeValue requestTimeout;

    /**
     * Interval at which forecasts are generated.
     */
    protected volatile TimeValue forecastInterval;

    /**
     * Delay duration before the forecast window begins.
     */
    protected volatile TimeValue forecastWindowDelay;

    /**
     * Maximum number of single stream forecasters allowed.
     */
    protected volatile Integer maxSingleStreamForecasters;

    /**
     * Maximum number of high-cardinality (HC) forecasters allowed.
     */
    protected volatile Integer maxHCForecasters;

    /**
     * Maximum number of features to be used for forecasting.
     */
    protected volatile Integer maxForecastFeatures;

    /**
     * Maximum number of categorical fields allowed.
     */
    protected volatile Integer maxCategoricalFields;

    /**
     * Constructor for the base class for validating and indexing forecast REST handlers.
     *
     * @param settings       Settings for the forecast plugin.
     * @param clusterService Cluster service.
     */
    public AbstractForecasterAction(Settings settings, ClusterService clusterService) {
        this.requestTimeout = FORECAST_REQUEST_TIMEOUT.get(settings);
        this.forecastInterval = FORECAST_INTERVAL.get(settings);
        this.forecastWindowDelay = FORECAST_WINDOW_DELAY.get(settings);
        this.maxSingleStreamForecasters = MAX_SINGLE_STREAM_FORECASTERS.get(settings);
        this.maxHCForecasters = MAX_HC_FORECASTERS.get(settings);
        this.maxForecastFeatures = MAX_FORECAST_FEATURES;
        this.maxCategoricalFields = ForecastNumericSetting.maxCategoricalFields();
        // TODO: will add more cluster setting consumer later
        // TODO: inject ClusterSettings only if clusterService is only used to get ClusterSettings
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_REQUEST_TIMEOUT, it -> requestTimeout = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_INTERVAL, it -> forecastInterval = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FORECAST_WINDOW_DELAY, it -> forecastWindowDelay = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_SINGLE_STREAM_FORECASTERS, it -> maxSingleStreamForecasters = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_HC_FORECASTERS, it -> maxHCForecasters = it);
    }
}
