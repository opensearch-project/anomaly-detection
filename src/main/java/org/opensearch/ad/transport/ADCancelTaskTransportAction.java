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

import static org.opensearch.ad.constant.ADCommonMessages.HISTORICAL_ANALYSIS_CANCELLED;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.ad.task.ADTaskCancellationState;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class ADCancelTaskTransportAction extends
    TransportNodesAction<ADCancelTaskRequest, ADCancelTaskResponse, ADCancelTaskNodeRequest, ADCancelTaskNodeResponse> {
    private final Logger logger = LogManager.getLogger(ADCancelTaskTransportAction.class);
    private ADTaskManager adTaskManager;

    @Inject
    public ADCancelTaskTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ADTaskManager adTaskManager
    ) {
        super(
            ADCancelTaskAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ADCancelTaskRequest::new,
            ADCancelTaskNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ADCancelTaskNodeResponse.class
        );
        this.adTaskManager = adTaskManager;
    }

    @Override
    protected ADCancelTaskResponse newResponse(
        ADCancelTaskRequest request,
        List<ADCancelTaskNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new ADCancelTaskResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected ADCancelTaskNodeRequest newNodeRequest(ADCancelTaskRequest request) {
        return new ADCancelTaskNodeRequest(request);
    }

    @Override
    protected ADCancelTaskNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ADCancelTaskNodeResponse(in);
    }

    @Override
    protected ADCancelTaskNodeResponse nodeOperation(ADCancelTaskNodeRequest request) {
        String userName = request.getUserName();
        String detectorId = request.getId();
        String detectorTaskId = request.getDetectorTaskId();
        String reason = Optional.ofNullable(request.getReason()).orElse(HISTORICAL_ANALYSIS_CANCELLED);
        ADTaskCancellationState state = adTaskManager.cancelLocalTaskByDetectorId(detectorId, detectorTaskId, reason, userName);
        logger.debug("Cancelled AD task for detector: {}", request.getId());
        return new ADCancelTaskNodeResponse(clusterService.localNode(), state);
    }
}
