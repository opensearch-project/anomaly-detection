/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.common.exception.InternalFailure;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;

public class StopDetectorTransportAction extends HandledTransportAction<ActionRequest, StopDetectorResponse> {

    private static final Logger LOG = LogManager.getLogger(StopDetectorTransportAction.class);

    private final Client client;
    private final DiscoveryNodeFilterer nodeFilter;

    @Inject
    public StopDetectorTransportAction(
        TransportService transportService,
        DiscoveryNodeFilterer nodeFilter,
        ActionFilters actionFilters,
        Client client
    ) {
        super(StopDetectorAction.NAME, transportService, actionFilters, StopDetectorRequest::new);
        this.client = client;
        this.nodeFilter = nodeFilter;
    }

    @Override
    protected void doExecute(Task task, ActionRequest actionRequest, ActionListener<StopDetectorResponse> listener) {
        StopDetectorRequest request = StopDetectorRequest.fromActionRequest(actionRequest);
        String adID = request.getAdID();
        try {
            DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
            DeleteModelRequest modelDeleteRequest = new DeleteModelRequest(adID, dataNodes);
            client.execute(DeleteModelAction.INSTANCE, modelDeleteRequest, ActionListener.wrap(response -> {
                if (response.hasFailures()) {
                    LOG.warn("Cannot delete all models of detector {}", adID);
                    for (FailedNodeException failedNodeException : response.failures()) {
                        LOG.warn("Deleting models of node has exception", failedNodeException);
                    }
                    // if customers are using an updated detector and we haven't deleted old
                    // checkpoints, customer would have trouble
                    listener.onResponse(new StopDetectorResponse(false));
                } else {
                    LOG.info("models of detector {} get deleted", adID);
                    listener.onResponse(new StopDetectorResponse(true));
                }
            }, exception -> {
                LOG.error(new ParameterizedMessage("Deletion of detector [{}] has exception.", adID), exception);
                listener.onResponse(new StopDetectorResponse(false));
            }));
        } catch (Exception e) {
            LOG.error("Fail to stop detector " + adID, e);
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            listener.onFailure(new InternalFailure(adID, cause));
        }
    }
}
