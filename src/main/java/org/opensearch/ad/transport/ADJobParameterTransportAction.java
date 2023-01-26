package org.opensearch.ad.transport;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class ADJobParameterTransportAction extends HandledTransportAction<ExtensionActionRequest, ADJobParameterResponse> {
    protected ADJobParameterTransportAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<ExtensionActionRequest> adJobParameterRequestReader
    ) {
        super(actionName, transportService, actionFilters, adJobParameterRequestReader);
    }

    @Override
    protected void doExecute(Task task, ExtensionActionRequest request, ActionListener<ADJobParameterResponse> actionListener) {

    }
}
