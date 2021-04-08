/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

public class ADTaskProfileResponse extends BaseNodesResponse<ADTaskProfileNodeResponse> {

    public ADTaskProfileResponse(StreamInput in) throws IOException {
        super(new ClusterName(in), in.readList(ADTaskProfileNodeResponse::readNodeResponse), in.readList(FailedNodeException::new));
    }

    public ADTaskProfileResponse(ClusterName clusterName, List<ADTaskProfileNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<ADTaskProfileNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public List<ADTaskProfileNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(ADTaskProfileNodeResponse::readNodeResponse);
    }

}
