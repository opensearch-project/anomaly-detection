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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;

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
