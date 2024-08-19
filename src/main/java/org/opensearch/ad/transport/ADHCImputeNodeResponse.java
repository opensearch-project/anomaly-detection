/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class ADHCImputeNodeResponse extends BaseNodeResponse {

    Exception previousException;

    public ADHCImputeNodeResponse(DiscoveryNode node, Exception previousException) {
        super(node);
        this.previousException = previousException;
    }

    public ADHCImputeNodeResponse(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            this.previousException = in.readException();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (previousException != null) {
            out.writeBoolean(true);
            out.writeException(previousException);
        } else {
            out.writeBoolean(false);
        }
    }

    public static ADHCImputeNodeResponse readNodeResponse(StreamInput in) throws IOException {
        return new ADHCImputeNodeResponse(in);
    }

    public Exception getPreviousException() {
        return previousException;
    }
}
