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

public class CronResponse extends BaseNodesResponse<CronNodeResponse> implements ToXContentFragment {
    static String NODES_JSON_KEY = "nodes";

    public CronResponse(StreamInput in) throws IOException {
        super(in);
    }

    public CronResponse(ClusterName clusterName, List<CronNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    public List<CronNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(CronNodeResponse::readNodeResponse);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<CronNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(NODES_JSON_KEY);
        for (CronNodeResponse nodeResp : getNodes()) {
            nodeResp.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }
}
