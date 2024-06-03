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

package org.opensearch.timeseries.ratelimit;

import java.io.IOException;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.timeseries.model.IndexableResult;

public abstract class ResultWriteRequest<ResultType extends IndexableResult> extends QueuedRequest implements Writeable {
    private final ResultType result;
    // If resultIndex is null, result will be stored in default result index.
    private final String resultIndex;

    public ResultWriteRequest(long expirationEpochMs, String configId, RequestPriority priority, ResultType result, String resultIndex) {
        super(expirationEpochMs, configId, priority);
        this.result = result;
        this.resultIndex = resultIndex;
    }

    public ResultWriteRequest(StreamInput in, Writeable.Reader<ResultType> resultReader) throws IOException {
        this.result = resultReader.read(in);
        this.resultIndex = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        result.writeTo(out);
        out.writeOptionalString(resultIndex);
    }

    public ResultType getResult() {
        return result;
    }

    public String getResultIndex() {
        return resultIndex;
    }
}
