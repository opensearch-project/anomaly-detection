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

package test.org.opensearch.ad.util;

import java.time.Clock;

public class RandomModelStateConfig {
    private final Boolean fullModel;
    private final Float priority;
    private final String detectorId;
    private final Integer sampleSize;
    private final Clock clock;

    private RandomModelStateConfig(Builder builder) {
        this.fullModel = builder.fullModel;
        this.priority = builder.priority;
        this.detectorId = builder.detectorId;
        this.sampleSize = builder.sampleSize;
        this.clock = builder.clock;
    }

    public Boolean getFullModel() {
        return fullModel;
    }

    public Float getPriority() {
        return priority;
    }

    public String getDetectorId() {
        return detectorId;
    }

    public Integer getSampleSize() {
        return sampleSize;
    }

    public Clock getClock() {
        return clock;
    }

    public static class Builder {
        private Boolean fullModel = null;
        private Float priority = null;
        private String detectorId = null;
        private Integer sampleSize = null;
        private Clock clock = null;

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

        public RandomModelStateConfig build() {
            RandomModelStateConfig config = new RandomModelStateConfig(this);
            return config;
        }
    }
}
