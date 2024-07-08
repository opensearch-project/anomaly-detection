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
import java.util.ArrayList;
import java.util.List;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.ratelimit.ResultWriteRequest;

public class ResultBulkRequest<ResultType extends IndexableResult, ResultWriteRequestType extends ResultWriteRequest<ResultType>> extends
    ActionRequest
    implements
        Writeable {
    private final List<ResultWriteRequestType> results;

    public ResultBulkRequest() {
        results = new ArrayList<>();
    }

    public ResultBulkRequest(StreamInput in, Writeable.Reader<ResultWriteRequestType> reader) throws IOException {
        super(in);
        int size = in.readVInt();
        results = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            results.add(reader.read(in));
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (results.isEmpty()) {
            validationException = ValidateActions.addValidationError(CommonMessages.NO_REQUESTS_ADDED_ERR, validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(results.size());
        for (ResultWriteRequestType result : results) {
            result.writeTo(out);
        }

    }

    /**
     *
     * @return all of the results to send
     */
    public List<ResultWriteRequestType> getResults() {
        return results;
    }

    /**
     * Add result to send
     * @param resultWriteRequest The result write request
     */
    public void add(ResultWriteRequestType resultWriteRequest) {
        results.add(resultWriteRequest);
    }

    /**
     *
     * @return total index requests
     */
    public int numberOfActions() {
        return results.size();
    }
}
