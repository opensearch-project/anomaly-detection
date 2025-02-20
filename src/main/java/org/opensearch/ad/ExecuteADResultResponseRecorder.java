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

package org.opensearch.ad;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.commons.authuser.User;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.ExecuteResultResponseRecorder;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.transport.ResultResponse;
import org.opensearch.timeseries.transport.handler.ResultBulkIndexingHandler;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.client.Client;

public class ExecuteADResultResponseRecorder extends
    ExecuteResultResponseRecorder<ADIndex, ADIndexManagement, ADTaskCacheManager, ADTaskType, ADTask, ADTaskManager, AnomalyResult, ADProfileAction> {

    private static final Logger log = LogManager.getLogger(ExecuteADResultResponseRecorder.class);

    public ExecuteADResultResponseRecorder(
        ADIndexManagement indexManagement,
        ResultBulkIndexingHandler<AnomalyResult, ADIndex, ADIndexManagement> resultHandler,
        ADTaskManager taskManager,
        DiscoveryNodeFilterer nodeFilter,
        ThreadPool threadPool,
        Client client,
        NodeStateManager nodeStateManager,
        ADTaskCacheManager taskCacheManager,
        int rcfMinSamples
    ) {
        super(
            indexManagement,
            resultHandler,
            taskManager,
            nodeFilter,
            threadPool,
            TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME,
            client,
            nodeStateManager,
            taskCacheManager,
            rcfMinSamples,
            ADIndex.RESULT,
            AnalysisType.AD,
            ADProfileAction.INSTANCE
        );
    }

    @Override
    protected AnomalyResult createErrorResult(
        String configId,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executeEndTime,
        String errorMessage,
        User user
    ) {
        return new AnomalyResult(
            configId,
            null, // no task id
            new ArrayList<FeatureData>(),
            dataStartTime,
            dataEndTime,
            executeEndTime,
            Instant.now(),
            errorMessage,
            Optional.empty(), // single-stream detectors have no entity
            user,
            indexManagement.getSchemaVersion(resultIndex),
            null // no model id
        );
    }

    /**
     * Update real time task (one document per detector in state index). If the real-time task has no changes compared with local cache,
     * the task won't update. Task only updates when the state changed, or any error happened, or job stopped. Task is mainly consumed
     * by the front-end to track analysis status. For single-stream analyses, we embed model total updates in ResultResponse and
     * update state accordingly. For HC analysis, we won't wait for model finishing updating before returning a response to the job scheduler
     * since it might be long before all entities finish execution. So we don't embed model total updates in AnomalyResultResponse.
     * Instead, we issue a profile request to poll each model node and get the maximum total updates among all models.
     * @param response response returned from executing AnomalyResultAction
     * @param configId config Id
     */
    @Override
    protected void updateRealtimeTask(ResultResponse<AnomalyResult> response, String configId) {
        if (response.isHC() != null && response.isHC()) {
            if (taskManager.skipUpdateRealtimeTask(configId, response.getError())) {
                return;
            }
            delayedUpdate(response, configId);
        } else {
            log
                .debug(
                    "Update latest realtime task for single stream detector {}, total updates: {}",
                    configId,
                    response.getRcfTotalUpdates()
                );
            updateLatestRealtimeTask(
                configId,
                null,
                response.getRcfTotalUpdates(),
                response.getConfigIntervalInMinutes(),
                response.getError()
            );
        }
    }
}
