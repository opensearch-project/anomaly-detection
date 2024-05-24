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

package org.opensearch.timeseries.transport;

import java.io.IOException;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

/**
 *  StatsNodeRequest to get a nodes stat
 */
public class StatsNodeRequest extends TransportRequest {
    private StatsRequest request;

    /**
     * Constructor
     */
    public StatsNodeRequest() {
        super();
    }

    public StatsNodeRequest(StreamInput in) throws IOException {
        super(in);
        this.request = new StatsRequest(in);
    }

    /**
     * Constructor
     *
     * @param request ADStatsRequest
     */
    public StatsNodeRequest(StatsRequest request) {
        this.request = request;
    }

    /**
     * Get ADStatsRequest
     *
     * @return ADStatsRequest for this node
     */
    public StatsRequest getADStatsRequest() {
        return request;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }
}
