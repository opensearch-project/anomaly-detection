/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class BooleanNodeResponse extends BaseNodeResponse {
    private final boolean answer;

    public BooleanNodeResponse(StreamInput in) throws IOException {
        super(in);
        answer = in.readBoolean();
    }

    public BooleanNodeResponse(DiscoveryNode node, boolean answer) {
        super(node);
        this.answer = answer;
    }

    public boolean isAnswerTrue() {
        return answer;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(answer);
    }
}
