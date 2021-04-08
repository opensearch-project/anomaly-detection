/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.task.ADTaskCancellationState;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;

public class ADCancelTaskTransportAction extends
    TransportNodesAction<ADCancelTaskRequest, ADCancelTaskResponse, ADCancelTaskNodeRequest, ADCancelTaskNodeResponse> {
    private final Logger logger = LogManager.getLogger(ADCancelTaskTransportAction.class);
    private Client client;
    private ADTaskManager adTaskManager;

    @Inject
    public ADCancelTaskTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ADTaskManager adTaskManager,
        Client client
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
        this.client = client;
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
        String reason = "Task cancelled by user";
        String userName = request.getUserName();
        String detectorId = request.getDetectorId();
        ADTaskCancellationState state = adTaskManager.cancelLocalTaskByDetectorId(detectorId, reason, userName);
        logger.debug("Cancelled AD task for detector: {}", request.getDetectorId());
        return new ADCancelTaskNodeResponse(clusterService.localNode(), state);
    }
}
