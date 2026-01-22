/*
 * SPDX-License-Identifier: Apache-2.0
 * 
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 * 
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 * 
 */
package org.opensearch.ad.correlation;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Anomaly class for anomaly correlation.
 */
public final class Anomaly {
    // This uniquely identifies the emitting source of the anomaly (e.g., model id).
    private final String modelId;
    // The id of the detector.
    private final String configId;
    // The start time of the anomaly.
    private final Instant dataStartTime;
    // The end time of the anomaly.
    private final Instant dataEndTime;

    public Anomaly(String modelId, String configId, Instant dataStartTime, Instant dataEndTime) {
        this.modelId = Objects.requireNonNull(modelId, "modelId");
        this.configId = Objects.requireNonNull(configId, "configId");
        this.dataStartTime = Objects.requireNonNull(dataStartTime, "dataStartTime");
        this.dataEndTime = Objects.requireNonNull(dataEndTime, "dataEndTime");

        if (!dataEndTime.isAfter(dataStartTime)) {
            throw new IllegalArgumentException("dataEndTime must be after dataStartTime");
        }
    }

    public String getModelId() {
        return modelId;
    }

    public Instant getDataStartTime() {
        return dataStartTime;
    }

    public Instant getDataEndTime() {
        return dataEndTime;
    }

    public Duration getDuration() {
        return Duration.between(dataStartTime, dataEndTime);
    }

    public String getConfigId() {
        return configId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Anomaly anomaly = (Anomaly) o;
        return modelId.equals(anomaly.modelId) && dataStartTime.equals(anomaly.dataStartTime) && dataEndTime.equals(anomaly.dataEndTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, dataStartTime, dataEndTime);
    }

    @Override
    public String toString() {
        return "Anomaly{"
            + "id='"
            + modelId
            + '\''
            + ", detectorName='"
            + configId
            + '\''
            + ", dataStartTime="
            + dataStartTime
            + ", dataEndTime="
            + dataEndTime
            + '}';
    }
}
