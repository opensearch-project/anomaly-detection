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
import org.opensearch.ad.task.ADTaskCancellationState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class ADCancelTaskNodeResponse extends BaseNodeResponse {

    private ADTaskCancellationState state;

    public ADCancelTaskNodeResponse(DiscoveryNode node, ADTaskCancellationState state) {
        super(node);
        this.state = state;
    }

    public ADCancelTaskNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.state = in.readEnum(ADTaskCancellationState.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(state);
    }

    public static ADCancelTaskNodeResponse readNodeResponse(StreamInput in) throws IOException {
        return new ADCancelTaskNodeResponse(in);
    }

    public ADTaskCancellationState getState() {
        return state;
    }
}
