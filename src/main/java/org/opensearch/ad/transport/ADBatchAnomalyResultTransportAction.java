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
import org.opensearch.action.support.TransportAction;
import org.opensearch.ad.task.ADBatchTaskRunner;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.transport.TransportService;

import com.google.inject.Inject;

public class ADBatchAnomalyResultTransportAction extends TransportAction<ADBatchAnomalyResultRequest, ADBatchAnomalyResultResponse> {

    private final TransportService transportService;
    private final ADBatchTaskRunner adBatchTaskRunner;

    @Inject
    public ADBatchAnomalyResultTransportAction(
        ExtensionsRunner extensionsRunner,
        TaskManager taskManager,
        ActionFilters actionFilters,
        ADBatchTaskRunner adBatchTaskRunner
    ) {
        super(ADBatchAnomalyResultAction.NAME, actionFilters, taskManager);
        this.transportService = extensionsRunner.getSdkTransportService().getTransportService();
        this.adBatchTaskRunner = adBatchTaskRunner;
    }

    @Override
    protected void doExecute(Task task, ADBatchAnomalyResultRequest request, ActionListener<ADBatchAnomalyResultResponse> actionListener) {
        adBatchTaskRunner.run(request.getAdTask(), transportService, actionListener);
    }
}
