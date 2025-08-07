/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class ForecastRunOnceProfileNodeResponse extends BaseNodeResponse {
    private final boolean answer;
    private final String exceptionMsg;

    public ForecastRunOnceProfileNodeResponse(StreamInput in) throws IOException {
        super(in);
        answer = in.readBoolean();
        exceptionMsg = in.readOptionalString();
    }

    public ForecastRunOnceProfileNodeResponse(DiscoveryNode node, boolean answer, String exceptionMsg) {
        super(node);
        this.answer = answer;
        this.exceptionMsg = exceptionMsg;
    }

    public boolean isAnswerTrue() {
        return answer;
    }

    public String getExceptionMsg() {
        return exceptionMsg;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(answer);
        out.writeOptionalString(exceptionMsg);
    }
}
