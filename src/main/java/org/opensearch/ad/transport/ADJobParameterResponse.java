package org.opensearch.ad.transport;

import java.io.IOException;

import org.opensearch.action.ActionResponse;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.model.ExtensionJobParameter;

public class ADJobParameterResponse extends ActionResponse implements ToXContentObject {

    private final ExtensionJobParameter extensionJobParameter;

    public ADJobParameterResponse(StreamInput streamInput) {}

    @Override
    public void writeTo(StreamInput in) throws IOException {
        super(in);
        extensionJobParameter = new ExtensionJobParameter(in);
    }

    public ADJobRunnerResponse(ExtensionJobParameter extensionJobParameter){
        this.extensionJobParameter=extensionJobParameter;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        extensionJobParameter.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        return xContentBuilder.startObject().field(RestHandlerUtils.EXTENSION_JOB_PARAMETER, extensionJobParameter).endObject();
    }
}
