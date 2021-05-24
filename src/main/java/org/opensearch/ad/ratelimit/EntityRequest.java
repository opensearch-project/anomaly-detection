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

import java.util.Optional;

import org.opensearch.ad.model.Entity;

public class EntityRequest extends QueuedRequest {
    private final Entity entity;

    /**
     *
     * @param expirationEpochMs Expiry time of the request
     * @param detectorId Detector Id
     * @param priority the entity's priority
     * @param entity the entity's attributes
     */
    public EntityRequest(long expirationEpochMs, String detectorId, SegmentPriority priority, Entity entity) {
        super(expirationEpochMs, detectorId, priority);
        this.entity = entity;
    }

    public Entity getEntity() {
        return entity;
    }

    public Optional<String> getModelId() {
        return entity.getModelId(detectorId);
    }
}
