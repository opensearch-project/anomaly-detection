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

public class CheckpointMaintainRequest extends QueuedRequest {
    private String entityModelId;

    public CheckpointMaintainRequest(long expirationEpochMs, String detectorId, RequestPriority priority, String entityModelId) {
        super(expirationEpochMs, detectorId, priority);
        this.entityModelId = entityModelId;
    }

    public String getEntityModelId() {
        return entityModelId;
    }
}
