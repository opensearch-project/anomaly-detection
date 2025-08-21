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

package test.org.opensearch.ad.util;

import java.time.Clock;
import java.time.Instant;

public class RandomModelStateConfig {
    private final Boolean fullModel;
    private final Float priority;
    private final String detectorId;
    private final Integer sampleSize;
    private final Clock clock;
    private final Boolean entityAttributes;
    private final Instant historyEnd;

    private RandomModelStateConfig(Builder builder) {
        this.fullModel = builder.fullModel;
        this.priority = builder.priority;
        this.detectorId = builder.detectorId;
        this.sampleSize = builder.sampleSize;
        this.clock = builder.clock;
        this.entityAttributes = builder.entityAttributes;
        this.historyEnd = builder.historyEnd;
    }

    public Boolean getFullModel() {
        return fullModel;
    }

    public Float getPriority() {
        return priority;
    }

    public String getId() {
        return detectorId;
    }

    public Integer getSampleSize() {
        return sampleSize;
    }

    public Clock getClock() {
        return clock;
    }

    public Boolean hasEntityAttributes() {
        return entityAttributes;
    }

    public Instant getHistoryEnd() {
        return historyEnd;
    }

    public static class Builder {
        private Boolean fullModel = null;
        private Float priority = null;
        private String detectorId = null;
        private Integer sampleSize = null;
        private Clock clock = null;
        private Boolean entityAttributes = false;
        private Instant historyEnd = null;

        public Builder fullModel(boolean fullModel) {
            this.fullModel = fullModel;
            return this;
        }

        public Builder priority(float priority) {
            this.priority = priority;
            return this;
        }

        public Builder detectorId(String detectorId) {
            this.detectorId = detectorId;
            return this;
        }

        public Builder sampleSize(int sampleSize) {
            this.sampleSize = sampleSize;
            return this;
        }

        public Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder entityAttributes(Boolean entityAttributes) {
            this.entityAttributes = entityAttributes;
            return this;
        }

        public Builder historyEnd(Instant historyEnd) {
            this.historyEnd = historyEnd;
            return this;
        }

        public RandomModelStateConfig build() {
            RandomModelStateConfig config = new RandomModelStateConfig(this);
            return config;
        }
    }
}
