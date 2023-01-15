package org.opensearch.ad.transport;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class ADJobRunnerTransportAction extends HandledTransportAction<ADJobRunnerRequest, ADJobRunnerResponse> {
    protected ADJobRunnerTransportAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<ADJobRunnerRequest> adJobRunnerRequestReader
    ) {
        super(actionName, transportService, actionFilters, adJobRunnerRequestReader);
    }

    @Override
    protected void doExecute(Task task, ADJobRunnerRequest request, ActionListener<ADJobRunnerResponse> actionListener) {

    }
}
