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
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class CronNodeResponse extends BaseNodeResponse implements ToXContentObject {
    static String NODE_ID = "node_id";

    public CronNodeResponse(StreamInput in) throws IOException {
        super(in);
    }

    public CronNodeResponse(DiscoveryNode node) {
        super(node);
    }

    public static CronNodeResponse readNodeResponse(StreamInput in) throws IOException {

        return new CronNodeResponse(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NODE_ID, getNode().getId());
        builder.endObject();
        return builder;
    }
}
