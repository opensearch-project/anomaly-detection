/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.ad.transport;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

public class DeleteModelResponse extends BaseNodesResponse<DeleteModelNodeResponse> implements ToXContentFragment {
    static String NODES_JSON_KEY = "nodes";

    public DeleteModelResponse(StreamInput in) throws IOException {
        super(in);
    }

    public DeleteModelResponse(ClusterName clusterName, List<DeleteModelNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    public List<DeleteModelNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(DeleteModelNodeResponse::readNodeResponse);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<DeleteModelNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(NODES_JSON_KEY);
        for (DeleteModelNodeResponse nodeResp : getNodes()) {
            nodeResp.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }
}
