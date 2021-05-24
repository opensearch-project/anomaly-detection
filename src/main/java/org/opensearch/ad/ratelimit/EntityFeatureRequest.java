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

import org.opensearch.ad.model.Entity;

public class EntityFeatureRequest extends EntityRequest {
    private final double[] currentFeature;
    private final long dataStartTimeMillis;

    public EntityFeatureRequest(
        long expirationEpochMs,
        String detectorId,
        SegmentPriority priority,
        Entity entity,
        double[] currentFeature,
        long dataStartTimeMs
    ) {
        super(expirationEpochMs, detectorId, priority, entity);
        this.currentFeature = currentFeature;
        this.dataStartTimeMillis = dataStartTimeMs;
    }

    public double[] getCurrentFeature() {
        return currentFeature;
    }

    public long getDataStartTimeMillis() {
        return dataStartTimeMillis;
    }
}
