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

package com.amazon.opendistroforelasticsearch.ad.model;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;

/**
 * Profile output for detector entity.
 */
public class EntityProfile implements Writeable, ToXContent, Mergeable {
    // field name in toXContent
    public static final String CATEGORY_FIELD = "category_field";
    public static final String ENTITY_VALUE = "value";
    public static final String IS_ACTIVE = "is_active";
    public static final String LAST_ACTIVE_TIMESTAMP = "last_active_timestamp";
    public static final String LAST_SAMPLE_TIMESTAMP = "last_sample_timestamp";

    private final String categoryField;
    private final String value;
    private Boolean isActive;
    private long lastActiveTimestampMs;
    private long lastSampleTimestampMs;
    private InitProgressProfile initProgress;
    private ModelProfile modelProfile;
    private EntityState state;

    public EntityProfile(
        String categoryField,
        String value,
        Boolean isActive,
        long lastActiveTimeStamp,
        long lastSampleTimestamp,
        InitProgressProfile initProgress,
        ModelProfile modelProfile,
        EntityState state
    ) {
        super();
        this.categoryField = categoryField;
        this.value = value;
        this.isActive = isActive;
        this.lastActiveTimestampMs = lastActiveTimeStamp;
        this.lastSampleTimestampMs = lastSampleTimestamp;
        this.initProgress = initProgress;
        this.modelProfile = modelProfile;
        this.state = state;
    }

    public static class Builder {
        private final String categoryField;
        private final String value;
        private Boolean isActive = null;
        private long lastActiveTimestampMs = -1L;
        private long lastSampleTimestampMs = -1L;
        private InitProgressProfile initProgress = null;
        private ModelProfile modelProfile = null;
        private EntityState state = EntityState.UNKNOWN;

        public Builder(String categoryField, String value) {
            this.categoryField = categoryField;
            this.value = value;
        }

        public Builder isActive(Boolean isActive) {
            this.isActive = isActive;
            return this;
        }

        public Builder lastActiveTimestampMs(long lastActiveTimestampMs) {
            this.lastActiveTimestampMs = lastActiveTimestampMs;
            return this;
        }

        public Builder lastSampleTimestampMs(long lastSampleTimestampMs) {
            this.lastSampleTimestampMs = lastSampleTimestampMs;
            return this;
        }

        public Builder initProgress(InitProgressProfile initProgress) {
            this.initProgress = initProgress;
            return this;
        }

        public Builder modelProfile(ModelProfile modelProfile) {
            this.modelProfile = modelProfile;
            return this;
        }

        public Builder state(EntityState state) {
            this.state = state;
            return this;
        }

        public EntityProfile build() {
            return new EntityProfile(
                categoryField,
                value,
                isActive,
                lastActiveTimestampMs,
                lastSampleTimestampMs,
                initProgress,
                modelProfile,
                state
            );
        }
    }

    public EntityProfile(StreamInput in) throws IOException {
        this.categoryField = in.readString();
        this.value = in.readString();
        this.isActive = in.readOptionalBoolean();
        this.lastActiveTimestampMs = in.readLong();
        this.lastSampleTimestampMs = in.readLong();
        if (in.readBoolean()) {
            this.initProgress = new InitProgressProfile(in);
        }
        if (in.readBoolean()) {
            this.modelProfile = new ModelProfile(in);
        }
        this.state = in.readEnum(EntityState.class);
    }

    public String getCategoryField() {
        return categoryField;
    }

    public String getValue() {
        return value;
    }

    public Optional<Boolean> getActive() {
        return Optional.ofNullable(isActive);
    }

    /**
    * Return the last active time of an entity's state.
    *
    * If the entity's state is active in the cache, the value indicates when the cache
    * is lastly accessed (get/put).  If the entity's state is inactive in the cache,
    * the value indicates when the cache state is created or when the entity is evicted
    * from active entity cache.
    *
    * @return the last active time of an entity's state
    */
    public Long getLastActiveTimestamp() {
        return lastActiveTimestampMs;
    }

    /**
     *
     * @return last document's timestamp belonging to an entity
     */
    public Long getLastSampleTimestamp() {
        return lastSampleTimestampMs;
    }

    public InitProgressProfile getInitProgress() {
        return initProgress;
    }

    public ModelProfile getModelProfile() {
        return modelProfile;
    }

    public EntityState getState() {
        return state;
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CATEGORY_FIELD, categoryField);
        builder.field(ENTITY_VALUE, value);
        if (isActive != null) {
            builder.field(IS_ACTIVE, isActive);
        }
        if (lastActiveTimestampMs > 0) {
            builder.field(LAST_ACTIVE_TIMESTAMP, lastActiveTimestampMs);
        }
        if (lastSampleTimestampMs > 0) {
            builder.field(LAST_SAMPLE_TIMESTAMP, lastSampleTimestampMs);
        }
        if (initProgress != null) {
            builder.field(CommonName.INIT_PROGRESS, initProgress);
        }
        if (modelProfile != null) {
            builder.field(CommonName.MODEL, modelProfile);
        }
        if (state != null && state != EntityState.UNKNOWN) {
            builder.field(CommonName.STATE, state);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(categoryField);
        out.writeString(value);
        out.writeOptionalBoolean(isActive);
        out.writeLong(lastActiveTimestampMs);
        out.writeLong(lastSampleTimestampMs);
        if (initProgress != null) {
            out.writeBoolean(true);
            initProgress.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        if (modelProfile != null) {
            out.writeBoolean(true);
            modelProfile.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeEnum(state);
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.append(CATEGORY_FIELD, categoryField);
        builder.append(ENTITY_VALUE, value);
        if (isActive != null) {
            builder.append(IS_ACTIVE, isActive);
        }
        if (lastActiveTimestampMs > 0) {
            builder.append(LAST_ACTIVE_TIMESTAMP, lastActiveTimestampMs);
        }
        if (lastSampleTimestampMs > 0) {
            builder.append(LAST_SAMPLE_TIMESTAMP, lastSampleTimestampMs);
        }
        if (initProgress != null) {
            builder.append(CommonName.INIT_PROGRESS, initProgress);
        }
        if (modelProfile != null) {
            builder.append(CommonName.MODELS, modelProfile);
        }
        if (state != null && state != EntityState.UNKNOWN) {
            builder.append(CommonName.STATE, state);
        }
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
        if (obj instanceof EntityProfile) {
            EntityProfile other = (EntityProfile) obj;
            EqualsBuilder equalsBuilder = new EqualsBuilder();
            equalsBuilder.append(categoryField, other.categoryField);
            equalsBuilder.append(value, other.value);
            equalsBuilder.append(isActive, other.isActive);
            equalsBuilder.append(lastActiveTimestampMs, other.lastActiveTimestampMs);
            equalsBuilder.append(lastSampleTimestampMs, other.lastSampleTimestampMs);
            equalsBuilder.append(initProgress, other.initProgress);
            equalsBuilder.append(modelProfile, other.modelProfile);
            equalsBuilder.append(state, other.state);

            return equalsBuilder.isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
            .append(categoryField)
            .append(value)
            .append(isActive)
            .append(lastActiveTimestampMs)
            .append(lastSampleTimestampMs)
            .append(initProgress)
            .append(modelProfile)
            .append(state)
            .toHashCode();
    }

    @Override
    public void merge(Mergeable other) {
        if (this == other || other == null || getClass() != other.getClass()) {
            return;
        }
        EntityProfile otherProfile = (EntityProfile) other;

        if (otherProfile.getInitProgress() != null) {
            this.initProgress = otherProfile.getInitProgress();
        }
        if (otherProfile.isActive != null) {
            this.isActive = otherProfile.isActive;
        }
        if (otherProfile.lastActiveTimestampMs > 0) {
            this.lastActiveTimestampMs = otherProfile.lastActiveTimestampMs;
        }
        if (otherProfile.lastSampleTimestampMs > 0) {
            this.lastSampleTimestampMs = otherProfile.lastSampleTimestampMs;
        }
        if (otherProfile.modelProfile != null) {
            this.modelProfile = otherProfile.modelProfile;
        }
        if (otherProfile.getState() != null && otherProfile.getState() != EntityState.UNKNOWN) {
            this.state = otherProfile.getState();
        }
    }
}
