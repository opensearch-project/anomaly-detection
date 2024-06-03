/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest.handler;

import static org.opensearch.ad.constant.ADCommonName.DETECTION_STATE_INDEX;
import static org.opensearch.ad.model.ADTaskType.HISTORICAL_DETECTOR_TASK_TYPES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_REQUEST_TIMEOUT;

import java.util.List;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.ad.ExecuteADResultResponseRecorder;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultRequest;
import org.opensearch.ad.transport.StopDetectorAction;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.rest.handler.IndexJobActionHandler;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.transport.ResultRequest;
import org.opensearch.transport.TransportService;

public class ADIndexJobActionHandler extends
    IndexJobActionHandler<ADIndex, ADIndexManagement, ADTaskCacheManager, ADTaskType, ADTask, ADTaskManager, AnomalyResult, ADProfileAction, ExecuteADResultResponseRecorder> {

    public ADIndexJobActionHandler(
        Client client,
        ADIndexManagement indexManagement,
        NamedXContentRegistry xContentRegistry,
        ADTaskManager adTaskManager,
        ExecuteADResultResponseRecorder recorder,
        NodeStateManager nodeStateManager,
        Settings settings
    ) {
        super(
            client,
            indexManagement,
            xContentRegistry,
            adTaskManager,
            recorder,
            AnomalyResultAction.INSTANCE,
            AnalysisType.AD,
            DETECTION_STATE_INDEX,
            StopDetectorAction.INSTANCE,
            nodeStateManager,
            settings,
            AD_REQUEST_TIMEOUT
        );
    }

    @Override
    protected ResultRequest createResultRequest(String configID, long start, long end) {
        return new AnomalyResultRequest(configID, start, end);
    }

    @Override
    protected List<ADTaskType> getBatchConfigTaskTypes() {
        return HISTORICAL_DETECTOR_TASK_TYPES;
    }

    /**
     * Stop config.
     * For realtime config, will set job as disabled.
     * For historical config, will set its task as cancelled.
     *
     * @param configId config id
     * @param historical stop historical analysis or not
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    @Override
    public void stopConfig(
        String configId,
        boolean historical,
        User user,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        // make sure detector exists
        nodeStateManager.getConfig(configId, AnalysisType.AD, (config) -> {
            if (!config.isPresent()) {
                listener.onFailure(new OpenSearchStatusException(CommonMessages.FAIL_TO_FIND_CONFIG_MSG + configId, RestStatus.NOT_FOUND));
                return;
            }
            if (historical) {
                // stop historical analyis
                taskManager
                    .getAndExecuteOnLatestConfigLevelTask(
                        configId,
                        getBatchConfigTaskTypes(),
                        (task) -> taskManager.stopHistoricalAnalysis(configId, task, user, listener),
                        transportService,
                        true,// reset task state when stop config
                        listener
                    );
            } else {
                // stop realtime detector job
                stopJob(configId, transportService, listener);
            }
        }, listener);
    }

}
