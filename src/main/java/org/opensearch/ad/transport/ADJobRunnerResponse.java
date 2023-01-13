package org.opensearch.ad.transport;

import java.io.IOException;
import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class ADJobRunnerResponse extends ActionResponse {


    public ADJobRunnerResponse(StreamInput streamInput) {
    }

    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {

    }
}
