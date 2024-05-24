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

package org.opensearch.timeseries.transport;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * StatsNodesResponse consists of the aggregated responses from the nodes
 */
public class StatsNodesResponse extends BaseNodesResponse<StatsNodeResponse> implements ToXContentObject {

    private static final String NODES_KEY = "nodes";

    /**
     * Constructor
     *
     * @param in StreamInput
     * @throws IOException thrown when unable to read from stream
     */
    public StatsNodesResponse(StreamInput in) throws IOException {
        super(new ClusterName(in), in.readList(StatsNodeResponse::readStats), in.readList(FailedNodeException::new));
    }

    /**
     * Constructor
     *
     * @param clusterName name of cluster
     * @param nodes List of StatsNodeResponse from nodes
     * @param failures List of failures from nodes
     */
    public StatsNodesResponse(ClusterName clusterName, List<StatsNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<StatsNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public List<StatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(StatsNodeResponse::readStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        String nodeId;
        DiscoveryNode node;
        builder.startObject(NODES_KEY);
        for (StatsNodeResponse adStats : getNodes()) {
            node = adStats.getNode();
            nodeId = node.getId();
            builder.startObject(nodeId);
            adStats.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
