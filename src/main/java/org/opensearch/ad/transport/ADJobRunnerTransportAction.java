package org.opensearch.ad.transport;

import static org.opensearch.ad.util.RestHandlerUtils.wrapRestActionListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.AnomalyDetectorJobRunner;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.extensions.action.ExtensionActionResponse;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.transport.JobRunnerRequest;
import org.opensearch.jobscheduler.transport.JobRunnerResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class ADJobRunnerTransportAction extends HandledTransportAction<ExtensionActionRequest, ExtensionActionResponse> {

    private static final Logger LOG = LogManager.getLogger(ADJobRunnerTransportAction.class);

    private final NamedXContentRegistry xContentRegistry;

    protected ADJobRunnerTransportAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<ExtensionActionRequest> adJobRunnerRequestReader,
        NamedXContentRegistry xContentRegistry
    ) {
        super(actionName, transportService, actionFilters, adJobRunnerRequestReader);
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected void doExecute(Task task, ExtensionActionRequest request, ActionListener<ExtensionActionResponse> actionListener) {
        String errorMessage = "Failed to run the Job";
        ActionListener<ExtensionActionResponse> listener = wrapRestActionListener(actionListener, errorMessage);
        JobRunnerRequest jobRunnerRequest = null;
        try {
            jobRunnerRequest = new JobRunnerRequest(request.getRequestBytes());
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }

        try {

            ScheduledJobParameter scheduledJobParameter = jobRunnerRequest.getJobParameter();
            JobExecutionContext jobExecutionContext = jobRunnerRequest.getJobExecutionContext();

            AnomalyDetectorJobRunner.getJobRunnerInstance().runJob(scheduledJobParameter, jobExecutionContext);
            JobRunnerResponse jobRunnerResponse = new JobRunnerResponse(true);
            listener.onResponse(new ExtensionJobActionResponse<JobRunnerResponse>(jobRunnerResponse));
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }

    }
}
