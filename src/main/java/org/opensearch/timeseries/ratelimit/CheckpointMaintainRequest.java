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

public class CheckpointMaintainRequest extends QueuedRequest {
    private String modelId;

    public CheckpointMaintainRequest(long expirationEpochMs, String configId, RequestPriority priority, String entityModelId) {
        super(expirationEpochMs, configId, priority);
        this.modelId = entityModelId;
    }

    public String getModelId() {
        return modelId;
    }
}
