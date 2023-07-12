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

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

public class DeleteModelNodeResponse extends BaseNodeResponse implements ToXContentObject {
    static String NODE_ID = "node_id";

    public DeleteModelNodeResponse(StreamInput in) throws IOException {
        super(in);
    }

    public DeleteModelNodeResponse(DiscoveryNode node) {
        super(node);
    }

    public static DeleteModelNodeResponse readNodeResponse(StreamInput in) throws IOException {

        return new DeleteModelNodeResponse(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NODE_ID, getNode().getId());
        builder.endObject();
        return builder;
    }
}
