/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.BaseDeleteConfigTransportAction;
import org.opensearch.transport.TransportService;

public class DeleteForecasterTransportAction extends
    BaseDeleteConfigTransportAction<TaskCacheManager, ForecastTaskType, ForecastTask, ForecastIndex, ForecastIndexManagement, ForecastTaskManager, Forecaster> {

    @Inject
    public DeleteForecasterTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        NodeStateManager nodeStateManager,
        ForecastTaskManager taskManager
    ) {
        super(
            transportService,
            actionFilters,
            client,
            clusterService,
            settings,
            xContentRegistry,
            nodeStateManager,
            taskManager,
            DeleteForecasterAction.NAME,
            ForecastSettings.FORECAST_FILTER_BY_BACKEND_ROLES,
            AnalysisType.FORECAST,
            ForecastIndex.STATE.getIndexName(),
            Forecaster.class,
            ForecastTaskType.RUN_ONCE_TASK_TYPES
        );
    }
}
