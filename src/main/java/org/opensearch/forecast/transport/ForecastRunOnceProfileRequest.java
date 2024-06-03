/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class ForecastRunOnceProfileRequest extends BaseNodesRequest<ForecastRunOnceProfileRequest> {
    private String configId;

    public ForecastRunOnceProfileRequest(StreamInput in) throws IOException {
        super(in);
        configId = in.readString();
    }

    /**
     * Constructor
     *
     * @param configId config id
     */
    public ForecastRunOnceProfileRequest(String configId, DiscoveryNode... nodes) {
        super(nodes);
        this.configId = configId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(configId);
    }

    public String getConfigId() {
        return configId;
    }
}
