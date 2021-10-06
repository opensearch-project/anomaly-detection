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

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.task.ADBatchTaskRunner;
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class ADBatchAnomalyResultTransportAction extends HandledTransportAction<ADBatchAnomalyResultRequest, ADBatchAnomalyResultResponse> {

    private final TransportService transportService;
    private final ADBatchTaskRunner adBatchTaskRunner;

    @Inject
    public ADBatchAnomalyResultTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ADBatchTaskRunner adBatchTaskRunner
    ) {
        super(ADBatchAnomalyResultAction.NAME, transportService, actionFilters, ADBatchAnomalyResultRequest::new);
        this.transportService = transportService;
        this.adBatchTaskRunner = adBatchTaskRunner;
    }

    @Override
    protected void doExecute(Task task, ADBatchAnomalyResultRequest request, ActionListener<ADBatchAnomalyResultResponse> actionListener) {
        adBatchTaskRunner.run(request.getAdTask(), transportService, actionListener);
    }
}
