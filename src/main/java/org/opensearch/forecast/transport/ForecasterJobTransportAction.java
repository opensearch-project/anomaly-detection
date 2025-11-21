/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import static org.opensearch.forecast.constant.ForecastCommonMessages.FAIL_TO_START_FORECASTER;
import static org.opensearch.forecast.constant.ForecastCommonMessages.FAIL_TO_STOP_FORECASTER;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_FILTER_BY_BACKEND_ROLES;
import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_REQUEST_TIMEOUT;

import java.time.Clock;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.ExecuteForecastResultResponseRecorder;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.rest.handler.ForecastIndexJobActionHandler;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.BaseJobTransportAction;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class ForecasterJobTransportAction extends
    BaseJobTransportAction<ForecastIndex, ForecastIndexManagement, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastTaskManager, ForecastResult, ForecastProfileAction, ExecuteForecastResultResponseRecorder, ForecastIndexJobActionHandler, Forecaster> {

    @Inject
    public ForecasterJobTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        ForecastIndexJobActionHandler forecastIndexJobActionHandler
    ) {
        super(
            transportService,
            actionFilters,
            client,
            clusterService,
            settings,
            xContentRegistry,
            FORECAST_FILTER_BY_BACKEND_ROLES,
            ForecasterJobAction.NAME,
            FORECAST_REQUEST_TIMEOUT,
            FAIL_TO_START_FORECASTER,
            FAIL_TO_STOP_FORECASTER,
            Forecaster.class,
            forecastIndexJobActionHandler,
            Clock.systemUTC(), // inject cannot find clock due to OS limitation
            Forecaster.class
        );
    }
}
