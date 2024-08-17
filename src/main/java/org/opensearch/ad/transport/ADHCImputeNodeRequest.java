/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class ADHCImputeNodeRequest extends BaseNodeRequest {
    private final ADHCImputeRequest request;

    public ADHCImputeNodeRequest(StreamInput in) throws IOException {
        super(in);
        this.request = new ADHCImputeRequest(in);
    }

    public ADHCImputeNodeRequest(ADHCImputeRequest request) {
        this.request = request;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }

    public ADHCImputeRequest getRequest() {
        return request;
    }
}
