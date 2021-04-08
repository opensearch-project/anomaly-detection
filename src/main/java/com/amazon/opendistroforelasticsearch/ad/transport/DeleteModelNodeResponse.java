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

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

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
