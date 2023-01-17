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
import org.opensearch.timeseries.util.RestHandlerUtils;

public class SearchConfigInfoResponse extends ActionResponse implements ToXContentObject {
    private long count;
    private boolean nameExists;

    public SearchConfigInfoResponse(StreamInput in) throws IOException {
        super(in);
        count = in.readLong();
        nameExists = in.readBoolean();
    }

    public SearchConfigInfoResponse(long count, boolean nameExists) {
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
