/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class ForecastRunOnceProfileRequest extends BaseNodesRequest<ForecastRunOnceProfileRequest> {
    private String configId;

    public ForecastRunOnceProfileRequest(StreamInput in) throws IOException {
        super(in);
        configId = in.readString();
    }

    /*Important to have this constructor. Otherwise, OS silently ignore the broadcast request.*/
    public ForecastRunOnceProfileRequest() {
        super((String[]) null);
    }

    /*Important to have this constructor. Otherwise, OS silently ignore the broadcast request.*/
    public ForecastRunOnceProfileRequest(String configId, String... nodesIds) {
        super(nodesIds);
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
