package org.opensearch.ad.transport;

import org.opensearch.ad.model.AnomalyDetectorJob;
import static org.opensearch.ad.util.RestHandlerUtils.wrapRestActionListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.*;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.jobscheduler.transport.JobParameterRequest;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.jobscheduler.spi.JobDocVersion;


public class ADJobParameterTransportAction extends HandledTransportAction<ExtensionActionRequest, ADJobParameterResponse> {

    private static final Logger LOG = LogManager.getLogger(ADJobParameterTransportAction.class);

    private final NamedXContentRegistry xContentRegistry;

    protected ADJobParameterTransportAction(String actionName, TransportService transportService, ActionFilters actionFilters, Writeable.Reader<ExtensionActionRequest> extensionActionRequestReader,NamedXContentRegistry xContentRegistry) {
        super(actionName, transportService, actionFilters, extensionActionRequestReader);
        this.xContentRegistry=xContentRegistry;
    }


    @Override
    protected void doExecute(Task task, ExtensionActionRequest request, ActionListener<ADJobParameterResponse> actionListener) {

        String errorMessage = "Failed to parse the Job Parameter";
        ActionListener<ADJobParameterResponse> listener = wrapRestActionListener(actionListener, errorMessage);
        JobParameterRequest jobParameterRequest;
        try {
            jobParameterRequest = new JobParameterRequest(request.getRequestBytes());
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }

        try {

            XContentParser parser = XContentType.JSON.xContent()
                    .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, jobParameterRequest.getJobSource(),XContentType.JSON);
            ScheduledJobParameter scheduledJobParameter= AnomalyDetectorJob.parse(parser);
            listener.onResponse(new ADJobParameterResponse(new ExtensionJobParameter(scheduledJobParameter)));
        }catch (Exception e){
            LOG.error(e);
            listener.onFailure(e);
        }
    }
}
