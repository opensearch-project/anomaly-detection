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
