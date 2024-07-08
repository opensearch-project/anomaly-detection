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

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

public class StatsTimeSeriesResponse extends ActionResponse implements ToXContentObject {
    private StatsResponse statsResponse;

    public StatsTimeSeriesResponse(StreamInput in) throws IOException {
        super(in);
        statsResponse = new StatsResponse(in);
    }

    public StatsTimeSeriesResponse(StatsResponse adStatsResponse) {
        this.statsResponse = adStatsResponse;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        statsResponse.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        statsResponse.toXContent(builder, params);
        return builder;
    }

    public StatsResponse getAdStatsResponse() {
        return statsResponse;
    }
}
