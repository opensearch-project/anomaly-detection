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
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.ad.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.opensearch.action.ActionResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

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
