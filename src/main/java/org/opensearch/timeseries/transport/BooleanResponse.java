/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.constant.CommonName;

public class BooleanResponse extends BaseNodesResponse<BooleanNodeResponse> implements ToXContentFragment {
    private final boolean answer;

    public BooleanResponse(StreamInput in) throws IOException {
        super(in);
        answer = in.readBoolean();
    }

    public BooleanResponse(ClusterName clusterName, List<BooleanNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
        this.answer = nodes.stream().anyMatch(response -> response.isAnswerTrue());
        ;
    }

    public boolean isAnswerTrue() {
        return answer;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(answer);
    }

    @Override
    protected List<BooleanNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(BooleanNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<BooleanNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonName.ANSWER_FIELD, answer);
        return builder;
    }
}
