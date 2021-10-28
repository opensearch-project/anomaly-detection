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
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

public class ADResultBulkRequest extends ActionRequest implements Writeable {
    private final List<AnomalyResult> anomalyResults;
    static final String NO_REQUESTS_ADDED_ERR = "no requests added";

    public ADResultBulkRequest() {
        anomalyResults = new ArrayList<>();
    }

    public ADResultBulkRequest(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        anomalyResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            anomalyResults.add(new AnomalyResult(in));
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
        for (AnomalyResult result : anomalyResults) {
            result.writeTo(out);
        }
    }

    /**
     *
     * @return all of the results to send
     */
    public List<AnomalyResult> getAnomalyResults() {
        return anomalyResults;
    }

    /**
     * Add result to send
     * @param result The result
     */
    public void add(AnomalyResult result) {
        anomalyResults.add(result);
    }

    /**
     *
     * @return total index requests
     */
    public int numberOfActions() {
        return anomalyResults.size();
    }
}
