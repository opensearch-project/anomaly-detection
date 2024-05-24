/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast;

import java.time.Instant;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.rest.handler.ForecastIndexJobActionHandler;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.forecast.transport.ForecastProfileAction;
import org.opensearch.forecast.transport.ForecastResultAction;
import org.opensearch.forecast.transport.ForecastResultRequest;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.JobProcessor;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.ResultRequest;

public class ForecastJobProcessor extends
    JobProcessor<ForecastIndex, ForecastIndexManagement, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastTaskManager, ForecastResult, ForecastProfileAction, ExecuteForecastResultResponseRecorder, ForecastIndexJobActionHandler> {

    private static final Logger log = LogManager.getLogger(ForecastJobProcessor.class);

    private static ForecastJobProcessor INSTANCE;

    public static ForecastJobProcessor getInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (ForecastJobProcessor.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new ForecastJobProcessor();
            return INSTANCE;
        }
    }

    private ForecastJobProcessor() {
        // Singleton class, use getJobRunnerInstance method instead of constructor
        super(AnalysisType.FORECAST, TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME, ForecastResultAction.INSTANCE);
    }

    public void registerSettings(Settings settings) {
        super.registerSettings(settings, ForecastSettings.FORECAST_MAX_RETRY_FOR_END_RUN_EXCEPTION);
    }

    @Override
    protected ResultRequest createResultRequest(String configId, long start, long end) {
        return new ForecastResultRequest(configId, start, end);
    }

    @Override
    protected void validateResultIndexAndRunJob(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant executionStartTime,
        Instant executionEndTime,
        String configId,
        String user,
        List<String> roles,
        ExecuteForecastResultResponseRecorder recorder,
        Config detector
    ) {
        ActionListener<Boolean> listener = ActionListener.wrap(r -> { log.debug("Result index is valid"); }, e -> {
            Exception exception = new EndRunException(configId, e.getMessage(), false);
            handleException(jobParameter, lockService, lock, executionStartTime, executionEndTime, exception, recorder, detector);
        });
        String resultIndex = jobParameter.getCustomResultIndex();
        if (resultIndex == null) {
            indexManagement.validateDefaultResultIndexForBackendJob(configId, user, roles, () -> {
                listener.onResponse(true);
                runJob(jobParameter, lockService, lock, executionStartTime, executionEndTime, configId, user, roles, recorder, detector);
            }, listener);
        } else {
            indexManagement.validateCustomIndexForBackendJob(resultIndex, configId, user, roles, () -> {
                listener.onResponse(true);
                runJob(jobParameter, lockService, lock, executionStartTime, executionEndTime, configId, user, roles, recorder, detector);
            }, listener);
        }
    }
}
