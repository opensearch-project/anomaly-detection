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
import java.util.ArrayList;
import java.util.List;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.ad.ratelimit.ResultWriteRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

public class ADResultBulkRequest extends ActionRequest implements Writeable {
    private final List<ResultWriteRequest> anomalyResults;
    static final String NO_REQUESTS_ADDED_ERR = "no requests added";

    public ADResultBulkRequest() {
        anomalyResults = new ArrayList<>();
    }

    public ADResultBulkRequest(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        anomalyResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            anomalyResults.add(new ResultWriteRequest(in));
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (anomalyResults.isEmpty()) {
            validationException = ValidateActions.addValidationError(NO_REQUESTS_ADDED_ERR, validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(anomalyResults.size());
        for (ResultWriteRequest result : anomalyResults) {
            result.writeTo(out);
        }
    }

    /**
     *
     * @return all of the results to send
     */
    public List<ResultWriteRequest> getAnomalyResults() {
        return anomalyResults;
    }

    /**
     * Add result to send
     * @param resultWriteRequest The result write request
     */
    public void add(ResultWriteRequest resultWriteRequest) {
        anomalyResults.add(resultWriteRequest);
    }

    /**
     *
     * @return total index requests
     */
    public int numberOfActions() {
        return anomalyResults.size();
    }
}
