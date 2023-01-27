package org.opensearch.ad.transport;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.extensions.action.ExtensionActionResponse;
import org.opensearch.jobscheduler.transport.JobRunnerRequest;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class ADJobRunnerTransportAction extends HandledTransportAction<ExtensionActionRequest, ExtensionActionResponse> {
    protected ADJobRunnerTransportAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<ExtensionActionRequest> adJobRunnerRequestReader
    ) {
        super(actionName, transportService, actionFilters, adJobRunnerRequestReader);
    }

    @Override
    protected void doExecute(Task task, ExtensionActionRequest request, ActionListener<ExtensionActionResponse> actionListener) {
        JobRunnerRequest jobRunnerRequest = new JobRunnerRequest(request.getRequestBytes());
    }
}
