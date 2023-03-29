package org.opensearch.ad.transport;

import static org.opensearch.ad.util.RestHandlerUtils.wrapRestActionListener;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.ad.AnomalyDetectorJobRunner;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.transport.request.JobRunnerRequest;
import org.opensearch.jobscheduler.transport.response.JobRunnerResponse;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKNamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;

import com.google.inject.Inject;

/**
 * Transport Action to execute job runner of anomaly detection.
 */
public class ADJobRunnerTransportAction extends TransportAction<JobRunnerRequest, JobRunnerResponse> {

    private static final Logger LOG = LogManager.getLogger(ADJobRunnerTransportAction.class);

    private SDKRestClient client;
    private final SDKNamedXContentRegistry xContentRegistry;
    private final Settings settings;

    @Inject
    protected ADJobRunnerTransportAction(
        ExtensionsRunner extensionsRunner,
        ActionFilters actionFilters,
        TaskManager taskManager,
        SDKNamedXContentRegistry xContentRegistry,
        SDKRestClient client
    ) {
        super(ADJobRunnerAction.NAME, actionFilters, taskManager);
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.settings = extensionsRunner.getEnvironmentSettings();
    }

    @Override
    protected void doExecute(Task task, JobRunnerRequest request, ActionListener<JobRunnerResponse> actionListener) {
        String errorMessage = "Failed to run the Job";
        ActionListener<JobRunnerResponse> listener = wrapRestActionListener(actionListener, errorMessage);
        try {
            JobExecutionContext jobExecutionContext = request.getJobExecutionContext();
            String jobParameterDocumentId = jobExecutionContext.getJobId();
            if (jobParameterDocumentId == null || jobParameterDocumentId.isEmpty()) {
                listener.onFailure(new IllegalArgumentException("jobParameterDocumentId cannot be empty or null"));
            } else {
                CompletableFuture<AnomalyDetectorJob> inProgressFuture = new CompletableFuture<>();
                findById(jobParameterDocumentId, new ActionListener<>() {
                    @Override
                    public void onResponse(AnomalyDetectorJob anomalyDetectorJob) {
                        inProgressFuture.complete(anomalyDetectorJob);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.info("could not find AnomalyDetectorJob with id " + jobParameterDocumentId, e);
                        inProgressFuture.completeExceptionally(e);
                    }
                });

                try {
                    AnomalyDetectorJob scheduledJobParameter = inProgressFuture
                        .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings).getMillis(), TimeUnit.MILLISECONDS)
                        .join();

                    JobRunnerResponse jobRunnerResponse;
                    if (scheduledJobParameter != null && validateJobExecutionContext(jobExecutionContext)) {
                        jobRunnerResponse = new JobRunnerResponse(true);
                    } else {
                        jobRunnerResponse = new JobRunnerResponse(false);
                    }
                    listener.onResponse(jobRunnerResponse);
                    if (jobRunnerResponse.getJobRunnerStatus()) {
                        AnomalyDetectorJobRunner.getJobRunnerInstance().runJob(scheduledJobParameter, jobExecutionContext);
                    }
                } catch (CompletionException e) {
                    if (e.getCause() instanceof TimeoutException) {
                        logger.info(" Request timed out with an exception ", e);
                    } else {
                        throw e;
                    }
                } catch (Exception e) {
                    logger.info(" Could not find Job Parameter due to exception ", e);
                }

            }
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    private void findById(String jobParameterId, ActionListener<AnomalyDetectorJob> listener) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, jobParameterId);
        try {
            client.get(getRequest, ActionListener.wrap(response -> {
                if (!response.isExists()) {
                    listener.onFailure(null);
                } else {
                    try {
                        XContentParser parser = XContentType.JSON
                            .xContent()
                            .createParser(xContentRegistry.getRegistry(), LoggingDeprecationHandler.INSTANCE, response.getSourceAsString());
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                        listener.onResponse(AnomalyDetectorJob.parse(parser));
                    } catch (IOException e) {
                        logger.error("IOException occurred finding AnomalyDetectorJob for jobParameterId " + jobParameterId, e);
                        listener.onFailure(e);
                    }
                }
            }, exception -> {
                logger.error("Exception occurred finding AnomalyDetectorJob for jobParameterId " + jobParameterId, exception);
                listener.onFailure(exception);
            }));
        } catch (Exception e) {
            logger.error("Error occurred finding anomaly detector job with jobParameterId " + jobParameterId, e);
            listener.onFailure(e);
        }

    }

    private boolean validateJobExecutionContext(JobExecutionContext jobExecutionContext) {
        if (jobExecutionContext != null
            && jobExecutionContext.getJobId() != null
            && !jobExecutionContext.getJobId().isEmpty()
            && jobExecutionContext.getJobIndexName() != null
            && !jobExecutionContext.getJobIndexName().isEmpty()
            && jobExecutionContext.getExpectedExecutionTime() != null
            && jobExecutionContext.getJobVersion() != null) {
            return true;
        }
        return false;
    }
}
