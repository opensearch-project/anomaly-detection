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

import org.opensearch.action.ActionRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;

public abstract class ResultRequest extends ActionRequest implements ToXContentObject {
    protected String configId;
    // time range start and end. Unit: epoch milliseconds
    protected long start;
    protected long end;

    public ResultRequest(StreamInput in) throws IOException {
        super(in);
        configId = in.readString();
        start = in.readLong();
        end = in.readLong();
    }

    public ResultRequest(String configID, long start, long end) {
        super();
        this.configId = configID;
        this.start = start;
        this.end = end;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public String getConfigId() {
        return configId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(configId);
        out.writeLong(start);
        out.writeLong(end);
    }
}
