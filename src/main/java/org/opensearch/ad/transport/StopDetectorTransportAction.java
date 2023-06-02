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

import static org.opensearch.ad.constant.ADCommonMessages.FAIL_TO_STOP_DETECTOR;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.common.exception.InternalFailure;
import org.opensearch.transport.TransportService;

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
            LOG.error(FAIL_TO_STOP_DETECTOR + " " + adID, e);
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            listener.onFailure(new InternalFailure(adID, FAIL_TO_STOP_DETECTOR, cause));
        }
    }
}
