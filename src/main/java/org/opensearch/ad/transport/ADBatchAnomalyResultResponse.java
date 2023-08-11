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

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class ADBatchAnomalyResultResponse extends ActionResponse {
    public String nodeId;
    public boolean runTaskRemotely;

    public ADBatchAnomalyResultResponse(String nodeId, boolean runTaskRemotely) {
        this.nodeId = nodeId;
        this.runTaskRemotely = runTaskRemotely;
    }

    public ADBatchAnomalyResultResponse(StreamInput in) throws IOException {
        super(in);
        nodeId = in.readString();
        runTaskRemotely = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeBoolean(runTaskRemotely);
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isRunTaskRemotely() {
        return runTaskRemotely;
    }

}
