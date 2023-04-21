package org.opensearch.ad.transport;

import static org.opensearch.ad.util.RestHandlerUtils.wrapRestActionListener;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.jobscheduler.model.ExtensionJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.transport.request.JobParameterRequest;
import org.opensearch.jobscheduler.transport.response.JobParameterResponse;
import org.opensearch.sdk.SDKNamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;

import com.google.inject.Inject;

/**
 * Transport Action to parse Anomaly Detector
 */
public class ADJobParameterTransportAction extends TransportAction<JobParameterRequest, JobParameterResponse> {

    private static final Logger LOG = LogManager.getLogger(ADJobParameterTransportAction.class);

    private final SDKNamedXContentRegistry xContentRegistry;

    @Inject
    protected ADJobParameterTransportAction(
        ActionFilters actionFilters,
        TaskManager taskManager,
        SDKNamedXContentRegistry xContentRegistry
    ) {
        super(ADJobParameterAction.NAME, actionFilters, taskManager);
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected void doExecute(Task task, JobParameterRequest request, ActionListener<JobParameterResponse> actionListener) {

        String errorMessage = "Failed to parse the Job Parameter";
        ActionListener<JobParameterResponse> listener = wrapRestActionListener(actionListener, errorMessage);
        try {
            XContentParser parser = XContentHelper
                .createParser(
                    xContentRegistry.getRegistry(),
                    LoggingDeprecationHandler.INSTANCE,
                    request.getJobSource(),
                    XContentType.JSON
                );
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ScheduledJobParameter scheduledJobParameter = AnomalyDetectorJob.parse(parser);
            JobParameterResponse jobParameterResponse = new JobParameterResponse(new ExtensionJobParameter(scheduledJobParameter));
            listener.onResponse(jobParameterResponse);
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }
}
