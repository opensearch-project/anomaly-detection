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
import java.util.Optional;

import org.opensearch.action.ActionResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class ADResultBulkResponse extends ActionResponse {
    public static final String RETRY_REQUESTS_JSON_KEY = "retry_requests";

    private List<IndexRequest> retryRequests;

    /**
     *
     * @param retryRequests a list of requests to retry
     */
    public ADResultBulkResponse(List<IndexRequest> retryRequests) {
        this.retryRequests = retryRequests;
    }

    public ADResultBulkResponse() {
        this.retryRequests = null;
    }

    public ADResultBulkResponse(StreamInput in) throws IOException {
        int size = in.readInt();
        if (size > 0) {
            retryRequests = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                retryRequests.add(new IndexRequest(in));
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (retryRequests == null || retryRequests.size() == 0) {
            out.writeInt(0);
        } else {
            out.writeInt(retryRequests.size());
            for (IndexRequest result : retryRequests) {
                result.writeTo(out);
            }
        }
    }

    public boolean hasFailures() {
        return retryRequests != null && retryRequests.size() > 0;
    }

    public Optional<List<IndexRequest>> getRetryRequests() {
        return Optional.ofNullable(retryRequests);
    }
}
