package org.opensearch.ad.transport;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.jobscheduler.spi.JobDocVersion;

public class ADJobParameterRequest extends ActionRequest {

    /**
     * accessToken is the placeholder for the user Identity/access token to be used to perform validation prior to invoking the extension action
     */
    private final String accessToken;

    /**
     * jobSource is the index entry bytes reference from the registered job index
     */
    private final BytesReference jobSource;

    /**
     * id is the job Id
     */
    private final String id;

    /**
     * jobDocVersion is the metadata regarding this particular registered job
     */
    private final JobDocVersion jobDocVersion;

    public ADJobParameterRequest(StreamInput in) throws IOException {
        this.accessToken = in.readString();
        this.jobSource = in.readBytesReference();
        this.id = in.readString();
        this.jobDocVersion = new JobDocVersion(in);
    }

    public ADJobParameterRequest(byte[] requestParams) throws IOException {
        this(StreamInput.wrap(requestParams));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.accessToken);
        out.writeBytesReference(this.jobSource);
        out.writeString(this.id);
        this.jobDocVersion.writeTo(out);
    }

    public String getAccessToken() {
        return accessToken;
    }

    public BytesReference getJobSource() {
        return jobSource;
    }

    public String getId() {
        return id;
    }

    public JobDocVersion getJobDocVersion() {
        return jobDocVersion;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
