package org.opensearch.ad.transport;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class ADJobRunnerTransportAction extends HandledTransportAction<ExtensionActionRequest, ADJobRunnerResponse> {
    protected ADJobRunnerTransportAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<ExtensionActionRequest> adJobRunnerRequestReader
    ) {
        super(actionName, transportService, actionFilters, adJobRunnerRequestReader);
    }

    @Override
    protected void doExecute(Task task, ExtensionActionRequest request, ActionListener<ADJobRunnerResponse> actionListener) {

    }
}
