/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import java.io.IOException;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

public class ForecastRunOnceProfileNodeRequest extends TransportRequest {
    private final ForecastRunOnceProfileRequest request;

    public ForecastRunOnceProfileNodeRequest(StreamInput in) throws IOException {
        super(in);
        request = new ForecastRunOnceProfileRequest(in);
    }

    public ForecastRunOnceProfileNodeRequest(ForecastRunOnceProfileRequest request) {
        this.request = request;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }

    public String getConfigId() {
        return request.getConfigId();
    }
}
