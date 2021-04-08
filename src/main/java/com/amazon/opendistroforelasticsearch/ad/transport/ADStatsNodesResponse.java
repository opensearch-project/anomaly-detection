/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

/**
 * ADStatsNodesResponse consists of the aggregated responses from the nodes
 */
public class ADStatsNodesResponse extends BaseNodesResponse<ADStatsNodeResponse> implements ToXContentObject {

    private static final String NODES_KEY = "nodes";

    /**
     * Constructor
     *
     * @param in StreamInput
     * @throws IOException thrown when unable to read from stream
     */
    public ADStatsNodesResponse(StreamInput in) throws IOException {
        super(new ClusterName(in), in.readList(ADStatsNodeResponse::readStats), in.readList(FailedNodeException::new));
    }

    /**
     * Constructor
     *
     * @param clusterName name of cluster
     * @param nodes List of ADStatsNodeResponses from nodes
     * @param failures List of failures from nodes
     */
    public ADStatsNodesResponse(ClusterName clusterName, List<ADStatsNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<ADStatsNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public List<ADStatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(ADStatsNodeResponse::readStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        String nodeId;
        DiscoveryNode node;
        builder.startObject(NODES_KEY);
        for (ADStatsNodeResponse adStats : getNodes()) {
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
