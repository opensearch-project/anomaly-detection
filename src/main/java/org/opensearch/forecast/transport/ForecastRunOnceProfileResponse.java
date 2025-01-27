/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.constant.CommonName;

public class ForecastRunOnceProfileResponse extends BaseNodesResponse<ForecastRunOnceProfileNodeResponse> implements ToXContentFragment {
    private final boolean answer;
    private final String exceptionMsg;

    public ForecastRunOnceProfileResponse(StreamInput in) throws IOException {
        super(in);
        answer = in.readBoolean();
        exceptionMsg = in.readOptionalString();
    }

    public ForecastRunOnceProfileResponse(
        ClusterName clusterName,
        List<ForecastRunOnceProfileNodeResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
        this.answer = nodes.stream().anyMatch(response -> response.isAnswerTrue());
        // get combined errors
        this.exceptionMsg = nodes
            .stream()
            .map(ForecastRunOnceProfileNodeResponse::getExceptionMsg)
            .filter(Objects::nonNull)
            .filter(msg -> !msg.isEmpty())
            .collect(Collectors.joining("\n"));
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

    @Override
    protected List<ForecastRunOnceProfileNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(ForecastRunOnceProfileNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<ForecastRunOnceProfileNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonName.ANSWER_FIELD, answer);
        builder.field(CommonName.ERROR_FIELD, exceptionMsg);
        return builder;
    }
}
