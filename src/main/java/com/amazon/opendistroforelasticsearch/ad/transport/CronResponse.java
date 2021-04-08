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
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

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
