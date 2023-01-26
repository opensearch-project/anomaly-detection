package org.opensearch.ad.transport;

import java.io.IOException;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class ADJobRunnerResponse extends ActionResponse implements ToXContentObject {

    private final boolean response;

    public ADJobRunnerResponse(StreamInput in) {
        super(in);
        response=in.readBoolean();
    }

    public ADJobRunnerResponse(boolean response){
        this.response=response;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
       out.writeBoolean(response);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        return xContentBuilder.startObject().field("response",response).endObject();
    }
}
