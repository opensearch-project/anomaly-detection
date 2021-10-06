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
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class RCFPollingResponse extends ActionResponse implements ToXContentObject {
    public static final String TOTAL_UPDATES_KEY = "totalUpdates";

    private final long totalUpdates;

    public RCFPollingResponse(long totalUpdates) {
        this.totalUpdates = totalUpdates;
    }

    public RCFPollingResponse(StreamInput in) throws IOException {
        super(in);
        totalUpdates = in.readVLong();
    }

    public long getTotalUpdates() {
        return totalUpdates;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalUpdates);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TOTAL_UPDATES_KEY, totalUpdates);
        builder.endObject();
        return builder;
    }
}
