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

package org.opensearch.ad.transport;

import static org.opensearch.ad.constant.CommonErrorMessages.HISTORICAL_ANALYSIS_CANCELLED;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.ad.task.ADTaskCancellationState;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;

import com.google.inject.Inject;

public class ADCancelTaskTransportAction extends TransportAction<ADCancelTaskRequest, ADCancelTaskResponse> {
    private final Logger logger = LogManager.getLogger(ADCancelTaskTransportAction.class);
    private ADTaskManager adTaskManager;
    private SDKClusterService clusterService;

    @Inject
    public ADCancelTaskTransportAction(
        TaskManager taskManager,
        ActionFilters actionFilters,
        ADTaskManager adTaskManager,
        SDKClusterService clusterService
    ) {
        super(ADCancelTaskAction.NAME, actionFilters, taskManager);
        this.adTaskManager = adTaskManager;
        this.clusterService = clusterService;
    }

    protected ADCancelTaskResponse newResponse(
        ADCancelTaskRequest request,
        List<ADCancelTaskNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new ADCancelTaskResponse(clusterService.state().getClusterName(), responses, failures);
    }


    @Override
    protected void doExecute(Task task, ADCancelTaskRequest request, ActionListener<ADCancelTaskResponse> actionListener) {
        String userName = request.getUserName();
        String detectorId = request.getDetectorId();
        String detectorTaskId = request.getDetectorTaskId();
        String reason = Optional.ofNullable(request.getReason()).orElse(HISTORICAL_ANALYSIS_CANCELLED);
        ADTaskCancellationState state = adTaskManager.cancelLocalTaskByDetectorId(detectorId, detectorTaskId, reason, userName);
        logger.debug("Cancelled AD task for detector: {}", request.getDetectorId());
        ADCancelTaskNodeResponse adCancelTaskNodeResponse = new ADCancelTaskNodeResponse(clusterService.localNode(), state);
        actionListener.onResponse(newResponse(request, List.of(adCancelTaskNodeResponse), Collections.emptyList()));
    }
}
