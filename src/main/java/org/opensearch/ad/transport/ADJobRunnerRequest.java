package org.opensearch.ad.transport;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.jobscheduler.model.ExtensionJobParameter;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;

public class ADJobRunnerRequest extends ActionRequest {

    /**
     * accessToken is the placeholder for the user Identity/access token to be used to perform validation prior to invoking the extension action
     */
    private final String accessToken;

    /**
     * jobParameter is job index entry intended to be used to validate prior to job execution
     */
    private final ExtensionJobParameter jobParameter;

    /**
     * jobExecutionContext holds the metadata to configure a job execution
     */
    private final JobExecutionContext jobExecutionContext;

    public String getAccessToken() {
        return accessToken;
    }

    public ExtensionJobParameter getJobParameter() {
        return jobParameter;
    }

    public JobExecutionContext getJobExecutionContext() {
        return jobExecutionContext;
    }

    public ADJobRunnerRequest(byte[] requestParams) throws IOException {
        this(StreamInput.wrap(requestParams));
    }

    /**
     * Instantiates a new Job Runner Request
     *
     * @param jobParameter the ScheduledJobParameter to convert into a writeable ExtensionJobParameter
     * @param jobExecutionContext the context used to facilitate a job run
     */
    public ADJobRunnerRequest(String accessToken, ScheduledJobParameter jobParameter, JobExecutionContext jobExecutionContext) {
        this.accessToken = accessToken;
        this.jobParameter = new ExtensionJobParameter(jobParameter);
        this.jobExecutionContext = jobExecutionContext;
    }

    /**
     * Instantiates a new Job Runner Request from {@link StreamInput}
     *
     * @param in in bytes stream input used to de-serialize the message.
     * @throws IOException IOException when message de-serialization fails.
     */
    public ADJobRunnerRequest(StreamInput in) throws IOException {
        this.accessToken = in.readString();
        this.jobParameter = new ExtensionJobParameter(in);
        this.jobExecutionContext = new JobExecutionContext(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.accessToken);
        this.jobParameter.writeTo(out);
        this.jobExecutionContext.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
