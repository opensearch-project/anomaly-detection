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

package org.opensearch.forecast.transport;

import static org.opensearch.forecast.constant.ForecastCommonMessages.FAIL_TO_STOP_FORECASTER;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.common.exception.InternalFailure;
import org.opensearch.timeseries.transport.DeleteModelRequest;
import org.opensearch.timeseries.transport.StopConfigRequest;
import org.opensearch.timeseries.transport.StopConfigResponse;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.TransportService;

public class StopForecasterTransportAction extends HandledTransportAction<ActionRequest, StopConfigResponse> {

    private static final Logger LOG = LogManager.getLogger(StopForecasterTransportAction.class);

    private final Client client;
    private final DiscoveryNodeFilterer nodeFilter;

    @Inject
    public StopForecasterTransportAction(
        TransportService transportService,
        DiscoveryNodeFilterer nodeFilter,
        ActionFilters actionFilters,
        Client client
    ) {
        super(StopForecasterAction.NAME, transportService, actionFilters, StopConfigRequest::new);
        this.client = client;
        this.nodeFilter = nodeFilter;
    }

    @Override
    protected void doExecute(Task task, ActionRequest actionRequest, ActionListener<StopConfigResponse> listener) {
        StopConfigRequest request = StopConfigRequest.fromActionRequest(actionRequest);
        String configId = request.getConfigID();
        try {
            DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
            DeleteModelRequest modelDeleteRequest = new DeleteModelRequest(configId, dataNodes);
            client.execute(DeleteForecastModelAction.INSTANCE, modelDeleteRequest, ActionListener.wrap(response -> {
                if (response.hasFailures()) {
                    LOG.warn("Cannot delete all models of forecaster {}", configId);
                    for (FailedNodeException failedNodeException : response.failures()) {
                        LOG.warn("Deleting models of node has exception", failedNodeException);
                    }
                    // if customers are using an updated detector and we haven't deleted old
                    // checkpoints, customer would have trouble
                    listener.onResponse(new StopConfigResponse(false));
                } else {
                    LOG.info("models of forecaster {} get deleted", configId);
                    listener.onResponse(new StopConfigResponse(true));
                }
            }, exception -> {
                LOG.error(new ParameterizedMessage("Deletion of forecaster [{}] has exception.", configId), exception);
                listener.onResponse(new StopConfigResponse(false));
            }));
        } catch (Exception e) {
            LOG.error(FAIL_TO_STOP_FORECASTER + " " + configId, e);
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            listener.onFailure(new InternalFailure(configId, FAIL_TO_STOP_FORECASTER, cause));
        }
    }
}
