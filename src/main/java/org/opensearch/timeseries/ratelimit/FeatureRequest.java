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

import java.util.Optional;

import org.opensearch.timeseries.model.Entity;

public class FeatureRequest extends QueuedRequest {
    private final double[] currentFeature;
    private final long dataStartTimeMillis;
    protected final String modelId;
    private final Optional<Entity> entity;
    private final String taskId;

    // used in HC
    public FeatureRequest(
        long expirationEpochMs,
        String configId,
        RequestPriority priority,
        double[] currentFeature,
        long dataStartTimeMs,
        Entity entity,
        String taskId
    ) {
        super(expirationEpochMs, configId, priority);
        this.currentFeature = currentFeature;
        this.dataStartTimeMillis = dataStartTimeMs;
        this.modelId = entity.getModelId(configId).isEmpty() ? null : entity.getModelId(configId).get();
        this.entity = Optional.ofNullable(entity);
        this.taskId = taskId;
    }

    // used in single-stream
    public FeatureRequest(
        long expirationEpochMs,
        String configId,
        RequestPriority priority,
        String modelId,
        double[] currentFeature,
        long dataStartTimeMs,
        String taskId
    ) {
        super(expirationEpochMs, configId, priority);
        this.currentFeature = currentFeature;
        this.dataStartTimeMillis = dataStartTimeMs;
        this.modelId = modelId;
        this.entity = Optional.empty();
        this.taskId = taskId;
    }

    public double[] getCurrentFeature() {
        return currentFeature;
    }

    public long getDataStartTimeMillis() {
        return dataStartTimeMillis;
    }

    public String getModelId() {
        return modelId;
    }

    public Optional<Entity> getEntity() {
        return entity;
    }

    public String getTaskId() {
        return taskId;
    }

    public boolean isRunOnce() {
        return taskId != null;
    }
}
