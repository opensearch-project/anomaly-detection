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

import org.opensearch.action.update.UpdateRequest;

public class CheckpointWriteRequest extends QueuedRequest {
    private final UpdateRequest updateRequest;

    public CheckpointWriteRequest(long expirationEpochMs, String detectorId, RequestPriority priority, UpdateRequest updateRequest) {
        super(expirationEpochMs, detectorId, priority);
        this.updateRequest = updateRequest;
    }

    public UpdateRequest getUpdateRequest() {
        return updateRequest;
    }
}
