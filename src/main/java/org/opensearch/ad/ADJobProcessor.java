/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad;

import java.time.Instant;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.rest.handler.ADIndexJobActionHandler;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.JobProcessor;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.transport.ResultRequest;

public class ADJobProcessor extends
    JobProcessor<ADIndex, ADIndexManagement, ADTaskCacheManager, ADTaskType, ADTask, ADTaskManager, AnomalyResult, ADProfileAction, ExecuteADResultResponseRecorder, ADIndexJobActionHandler> {

    private static final Logger log = LogManager.getLogger(ADJobProcessor.class);

    private static ADJobProcessor INSTANCE;

    public static ADJobProcessor getInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (ADJobProcessor.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new ADJobProcessor();
            return INSTANCE;
        }
    }

    private ADJobProcessor() {
        // Singleton class, use getJobRunnerInstance method instead of constructor
        super(AnalysisType.AD, TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME, AnomalyResultAction.INSTANCE);
    }

    public void registerSettings(Settings settings) {
        super.registerSettings(settings, AnomalyDetectorSettings.AD_MAX_RETRY_FOR_END_RUN_EXCEPTION);
    }

    @Override
    protected ResultRequest createResultRequest(String configId, long start, long end) {
        return new AnomalyResultRequest(configId, start, end);
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
        ExecuteADResultResponseRecorder recorder,
        Config detector
    ) {
        String resultIndex = jobParameter.getCustomResultIndex();
        if (resultIndex == null) {
            runJob(jobParameter, lockService, lock, executionStartTime, executionEndTime, configId, user, roles, recorder, detector);
            return;
        }
        ActionListener<Boolean> listener = ActionListener.wrap(r -> { log.debug("Custom index is valid"); }, e -> {
            Exception exception = new EndRunException(configId, e.getMessage(), false);
            handleException(jobParameter, lockService, lock, executionStartTime, executionEndTime, exception, recorder, detector);
        });
        indexManagement.validateCustomIndexForBackendJob(resultIndex, configId, user, roles, () -> {
            listener.onResponse(true);
            runJob(jobParameter, lockService, lock, executionStartTime, executionEndTime, configId, user, roles, recorder, detector);
        }, listener);
    }
}
