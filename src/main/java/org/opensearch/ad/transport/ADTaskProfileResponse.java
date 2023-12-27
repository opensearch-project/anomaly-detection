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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

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

    public static ADTaskProfileResponse fromActionResponse(ActionResponse actionResponse) {
        if (actionResponse instanceof ADTaskProfileResponse) {
            return (ADTaskProfileResponse) actionResponse;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionResponse.writeTo(osso);
            try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new ADTaskProfileResponse(input);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to parse ActionResponse into ADTaskProfileResponse", e);
        }
    }
}
