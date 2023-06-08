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

package org.opensearch.ad.ratelimit;

import java.io.IOException;

import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

public class ResultWriteRequest extends QueuedRequest implements Writeable {
    private final AnomalyResult result;
    // If resultIndex is null, result will be stored in default result index.
    private final String resultIndex;

    public ResultWriteRequest(
        long expirationEpochMs,
        String detectorId,
        RequestPriority priority,
        AnomalyResult result,
        String resultIndex
    ) {
        super(expirationEpochMs, detectorId, priority);
        this.result = result;
        this.resultIndex = resultIndex;
    }

    public ResultWriteRequest(StreamInput in) throws IOException {
        this.result = new AnomalyResult(in);
        this.resultIndex = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        result.writeTo(out);
        out.writeOptionalString(resultIndex);
    }

    public AnomalyResult getResult() {
        return result;
    }

    public String getCustomResultIndex() {
        return resultIndex;
    }
}
