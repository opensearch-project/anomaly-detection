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
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

public class SearchAnomalyDetectorInfoResponse extends ActionResponse implements ToXContentObject {
    private long count;
    private boolean nameExists;

    public SearchAnomalyDetectorInfoResponse(StreamInput in) throws IOException {
        super(in);
        count = in.readLong();
        nameExists = in.readBoolean();
    }

    public SearchAnomalyDetectorInfoResponse(long count, boolean nameExists) {
        this.count = count;
        this.nameExists = nameExists;
    }

    public long getCount() {
        return count;
    }

    public boolean isNameExists() {
        return nameExists;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(count);
        out.writeBoolean(nameExists);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RestHandlerUtils.COUNT, count);
        builder.field(RestHandlerUtils.MATCH, nameExists);
        builder.endObject();
        return builder;
    }
}
