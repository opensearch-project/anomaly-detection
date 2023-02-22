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

import org.opensearch.action.ActionResponse;
import org.opensearch.ad.stats.ADStatsResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

public class StatsAnomalyDetectorResponse extends ActionResponse implements ToXContentObject {
    private ADStatsResponse adStatsResponse;

    public StatsAnomalyDetectorResponse(StreamInput in) throws IOException {
        super(in);
        adStatsResponse = new ADStatsResponse(in);
    }

    public StatsAnomalyDetectorResponse(ADStatsResponse adStatsResponse) {
        this.adStatsResponse = adStatsResponse;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        adStatsResponse.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        adStatsResponse.toXContent(builder, params);
        return builder;
    }

    protected ADStatsResponse getAdStatsResponse() {
        return adStatsResponse;
    }
}
