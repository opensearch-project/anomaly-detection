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

import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

/**
 *  ADStatsNodeRequest to get a nodes stat
 */
public class ADStatsNodeRequest extends BaseNodeRequest {
    private ADStatsRequest request;

    /**
     * Constructor
     */
    public ADStatsNodeRequest() {
        super();
    }

    public ADStatsNodeRequest(StreamInput in) throws IOException {
        super(in);
        this.request = new ADStatsRequest(in);
    }

    /**
     * Constructor
     *
     * @param request ADStatsRequest
     */
    public ADStatsNodeRequest(ADStatsRequest request) {
        this.request = request;
    }

    /**
     * Get ADStatsRequest
     *
     * @return ADStatsRequest for this node
     */
    public ADStatsRequest getADStatsRequest() {
        return request;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }
}
