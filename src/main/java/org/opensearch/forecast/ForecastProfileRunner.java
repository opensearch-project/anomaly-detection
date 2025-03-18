/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast;

import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskProfile;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.model.ForecasterProfile;
import org.opensearch.forecast.settings.ForecastNumericSetting;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.forecast.transport.ForecastProfileAction;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.ProfileRunner;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class ForecastProfileRunner extends
    ProfileRunner<TaskCacheManager, ForecastTaskType, ForecastTask, ForecastIndex, ForecastIndexManagement, ForecastTaskProfile, ForecastTaskManager, ForecasterProfile, ForecastProfileAction, ForecastTaskProfileRunner> {

    public ForecastProfileRunner(
        Client client,
        SecurityClientUtil clientUtil,
        NamedXContentRegistry xContentRegistry,
        DiscoveryNodeFilterer nodeFilter,
        long requiredSamples,
        TransportService transportService,
        ForecastTaskManager forecastTaskManager,
        ForecastTaskProfileRunner taskProfileRunner
    ) {
        super(
            client,
            clientUtil,
            xContentRegistry,
            nodeFilter,
            requiredSamples,
            transportService,
            forecastTaskManager,
            AnalysisType.FORECAST,
            ForecastTaskType.REALTIME_TASK_TYPES,
            ForecastTaskType.RUN_ONCE_TASK_TYPES,
            ForecastNumericSetting.maxCategoricalFields(),
            ProfileName.FORECAST_TASK,
            ForecastProfileAction.INSTANCE,
            Forecaster::parse,
            taskProfileRunner
        );
    }

    @Override
    protected ForecasterProfile.Builder createProfileBuilder() {
        return new ForecasterProfile.Builder();
    }

}
