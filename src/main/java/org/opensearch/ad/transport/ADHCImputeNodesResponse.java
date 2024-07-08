/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class ADHCImputeNodesResponse extends BaseNodesResponse<ADHCImputeNodeResponse> {
    public ADHCImputeNodesResponse(StreamInput in) throws IOException {
        super(new ClusterName(in), in.readList(ADHCImputeNodeResponse::readNodeResponse), in.readList(FailedNodeException::new));
    }

    public ADHCImputeNodesResponse(ClusterName clusterName, List<ADHCImputeNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<ADHCImputeNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(ADHCImputeNodeResponse::readNodeResponse);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<ADHCImputeNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

}
