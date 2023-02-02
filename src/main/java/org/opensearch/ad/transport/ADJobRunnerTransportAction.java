package org.opensearch.ad.transport;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.opensearch.action.get.GetRequest;
import org.opensearch.ad.model.AnomalyDetectorJob;
import static org.opensearch.ad.util.RestHandlerUtils.wrapRestActionListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.AnomalyDetectorJobRunner;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.extensions.action.ExtensionActionResponse;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.transport.ExtensionJobActionResponse;
import org.opensearch.jobscheduler.transport.JobRunnerRequest;
import org.opensearch.jobscheduler.transport.JobRunnerResponse;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport Action to execute job runner of anomaly detection.
 */
public class ADJobRunnerTransportAction extends HandledTransportAction<ExtensionActionRequest, ExtensionActionResponse> {

    private static final Logger LOG = LogManager.getLogger(ADJobRunnerTransportAction.class);

    private SDKRestClient client;

    protected ADJobRunnerTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SDKRestClient client
    ) {
        super(ADJobRunnerAction.NAME, transportService, actionFilters, ExtensionActionRequest::new);
        this.client=client;
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
            final AnomalyDetectorJob[] scheduledJobParameter = new AnomalyDetectorJob[1];
            CompletableFuture<AnomalyDetectorJob[]> inProgressFuture = new CompletableFuture<>();

            String jobParameterDocumentId=jobRunnerRequest.getJobParameterDocumentId();
            findById(jobParameterDocumentId, new ActionListener<>() {
                        @Override
                        public void onResponse(AnomalyDetectorJob anomalyDetectorJob) {
                            scheduledJobParameter[0] =anomalyDetectorJob;
                            inProgressFuture.complete(scheduledJobParameter);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.info("could not find AnomalyDetectorJob with id "+jobParameterDocumentId, e);
                            inProgressFuture.completeExceptionally(e);
                        }
                    }
            );
            JobExecutionContext jobExecutionContext = jobRunnerRequest.getJobExecutionContext();

            AnomalyDetectorJobRunner.getJobRunnerInstance().runJob(scheduledJobParameter[0], jobExecutionContext);
            JobRunnerResponse jobRunnerResponse = new JobRunnerResponse(true);
            listener.onResponse(new ExtensionJobActionResponse<>(jobRunnerResponse));
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }

    }

    private void findById(String jobParameterId, ActionListener<AnomalyDetectorJob> listener) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, jobParameterId);

        client.get(getRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                listener.onResponse(null);
            } else {
                try {
                    XContentParser parser = XContentType.JSON.xContent()
                            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString());
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    listener.onResponse(AnomalyDetectorJob.parse(parser));
                } catch (IOException e) {
                    logger.error("IOException occurred finding AnomalyDetectorJob for jobParameterId " + jobParameterId, e);
                    listener.onResponse(null);
                }
            }
        }, exception -> {
            logger.error("Exception occurred finding AnomalyDetectorJob for jobParameterId " + jobParameterId, exception);
            listener.onFailure(exception);
        }));
    }
}
