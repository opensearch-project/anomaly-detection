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

public class EntityResultResponse extends ActionResponse {
    private long totalUpdates;

    public EntityResultResponse(long totalUpdates) {
        this.totalUpdates = totalUpdates;
    }

    public EntityResultResponse(StreamInput in) throws IOException {
        super(in);
        totalUpdates = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(totalUpdates);
    }

    public long getTotalUpdates() {
        return totalUpdates;
    }
}
