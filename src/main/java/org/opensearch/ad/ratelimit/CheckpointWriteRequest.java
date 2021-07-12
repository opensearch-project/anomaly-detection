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

import org.opensearch.action.index.IndexRequest;

public class CheckpointWriteRequest extends QueuedRequest {
    private final IndexRequest indexRequest;

    public CheckpointWriteRequest(long expirationEpochMs, String detectorId, RequestPriority priority, IndexRequest indexRequest) {
        super(expirationEpochMs, detectorId, priority);
        this.indexRequest = indexRequest;
    }

    public IndexRequest getIndexRequest() {
        return indexRequest;
    }
}
