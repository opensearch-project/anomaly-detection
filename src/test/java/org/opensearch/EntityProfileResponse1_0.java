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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.action.ActionResponse;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class EntityProfileResponse1_0 extends ActionResponse implements ToXContentObject {
    public static final String ACTIVE = "active";
    public static final String LAST_ACTIVE_TS = "last_active_timestamp";
    public static final String TOTAL_UPDATES = "total_updates";
    private final Boolean isActive;
    private final long lastActiveMs;
    private final long totalUpdates;
    private final ModelProfile1_0 modelProfile;

    public static class Builder {
        private Boolean isActive = null;
        private long lastActiveMs = -1L;
        private long totalUpdates = -1L;
        private ModelProfile1_0 modelProfile = null;

        public Builder() {}

        public Builder setActive(Boolean isActive) {
            this.isActive = isActive;
            return this;
        }

        public Builder setLastActiveMs(long lastActiveMs) {
            this.lastActiveMs = lastActiveMs;
            return this;
        }

        public Builder setTotalUpdates(long totalUpdates) {
            this.totalUpdates = totalUpdates;
            return this;
        }

        public Builder setModelProfile(ModelProfile1_0 modelProfile) {
            this.modelProfile = modelProfile;
            return this;
        }

        public EntityProfileResponse1_0 build() {
            return new EntityProfileResponse1_0(isActive, lastActiveMs, totalUpdates, modelProfile);
        }
    }

    public EntityProfileResponse1_0(Boolean isActive, long lastActiveTimeMs, long totalUpdates, ModelProfile1_0 modelProfile) {
        this.isActive = isActive;
        this.lastActiveMs = lastActiveTimeMs;
        this.totalUpdates = totalUpdates;
        this.modelProfile = modelProfile;
    }

    public EntityProfileResponse1_0(StreamInput in) throws IOException {
        super(in);
        isActive = in.readOptionalBoolean();
        lastActiveMs = in.readLong();
        totalUpdates = in.readLong();
        if (in.readBoolean()) {
            modelProfile = new ModelProfile1_0(in);
        } else {
            modelProfile = null;
        }
    }

    public Optional<Boolean> isActive() {
        return Optional.ofNullable(isActive);
    }

    public long getLastActiveMs() {
        return lastActiveMs;
    }

    public long getTotalUpdates() {
        return totalUpdates;
    }

    public ModelProfile1_0 getModelProfile() {
        return modelProfile;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBoolean(isActive);
        out.writeLong(lastActiveMs);
        out.writeLong(totalUpdates);
        if (modelProfile != null) {
            out.writeBoolean(true);
            modelProfile.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (isActive != null) {
            builder.field(ACTIVE, isActive);
        }
        if (lastActiveMs >= 0) {
            builder.field(LAST_ACTIVE_TS, lastActiveMs);
        }
        if (totalUpdates >= 0) {
            builder.field(TOTAL_UPDATES, totalUpdates);
        }
        if (modelProfile != null) {
            builder.field(CommonName.MODEL, modelProfile);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.append(ACTIVE, isActive);
        builder.append(LAST_ACTIVE_TS, lastActiveMs);
        builder.append(TOTAL_UPDATES, totalUpdates);
        builder.append(CommonName.MODEL, modelProfile);

        return builder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        if (obj instanceof EntityProfileResponse1_0) {
            EntityProfileResponse1_0 other = (EntityProfileResponse1_0) obj;
            EqualsBuilder equalsBuilder = new EqualsBuilder();
            equalsBuilder.append(isActive, other.isActive);
            equalsBuilder.append(lastActiveMs, other.lastActiveMs);
            equalsBuilder.append(totalUpdates, other.totalUpdates);
            equalsBuilder.append(modelProfile, other.modelProfile);

            return equalsBuilder.isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(isActive).append(lastActiveMs).append(totalUpdates).append(modelProfile).toHashCode();
    }
}
