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

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.task.ADBatchTaskRunner;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class ADBatchTaskRemoteExecutionTransportAction extends
    HandledTransportAction<ADBatchAnomalyResultRequest, ADBatchAnomalyResultResponse> {

    private final ADBatchTaskRunner adBatchTaskRunner;
    private final TransportService transportService;

    @Inject
    public ADBatchTaskRemoteExecutionTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ADBatchTaskRunner adBatchTaskRunner
    ) {
        super(ADBatchTaskRemoteExecutionAction.NAME, transportService, actionFilters, ADBatchAnomalyResultRequest::new);
        this.adBatchTaskRunner = adBatchTaskRunner;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, ADBatchAnomalyResultRequest request, ActionListener<ADBatchAnomalyResultResponse> listener) {
        adBatchTaskRunner.startADBatchTaskOnWorkerNode(request.getAdTask(), true, transportService, listener);
    }
}
