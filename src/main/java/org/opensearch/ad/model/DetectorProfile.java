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

package org.opensearch.ad.model;

import java.io.IOException;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

public class DetectorProfile implements Writeable, ToXContentObject, Mergeable {
    private DetectorState state;
    private String error;
    private ModelProfileOnNode[] modelProfile;
    private int shingleSize;
    private String coordinatingNode;
    private long totalSizeInBytes;
    private InitProgressProfile initProgress;
    private Long totalEntities;
    private Long activeEntities;
    private ADTaskProfile adTaskProfile;
    private long modelCount;

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    public DetectorProfile(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            this.state = in.readEnum(DetectorState.class);
        }

        this.error = in.readOptionalString();
        this.modelProfile = in.readOptionalArray(ModelProfileOnNode::new, ModelProfileOnNode[]::new);
        this.shingleSize = in.readOptionalInt();
        this.coordinatingNode = in.readOptionalString();
        this.totalSizeInBytes = in.readOptionalLong();
        this.totalEntities = in.readOptionalLong();
        this.activeEntities = in.readOptionalLong();
        if (in.readBoolean()) {
            this.initProgress = new InitProgressProfile(in);
        }
        if (in.readBoolean()) {
            this.adTaskProfile = new ADTaskProfile(in);
        }
        this.modelCount = in.readVLong();
    }

    private DetectorProfile() {}

    public static class Builder {
        private DetectorState state = null;
        private String error = null;
        private ModelProfileOnNode[] modelProfile = null;
        private int shingleSize = -1;
        private String coordinatingNode = null;
        private long totalSizeInBytes = -1;
        private InitProgressProfile initProgress = null;
        private Long totalEntities;
        private Long activeEntities;
        private ADTaskProfile adTaskProfile;
        private long modelCount = 0;

        public Builder() {}

        public Builder state(DetectorState state) {
            this.state = state;
            return this;
        }

        public Builder error(String error) {
            this.error = error;
            return this;
        }

        public Builder modelProfile(ModelProfileOnNode[] modelProfile) {
            this.modelProfile = modelProfile;
            return this;
        }

        public Builder modelCount(long modelCount) {
            this.modelCount = modelCount;
            return this;
        }

        public Builder shingleSize(int shingleSize) {
            this.shingleSize = shingleSize;
            return this;
        }

        public Builder coordinatingNode(String coordinatingNode) {
            this.coordinatingNode = coordinatingNode;
            return this;
        }

        public Builder totalSizeInBytes(long totalSizeInBytes) {
            this.totalSizeInBytes = totalSizeInBytes;
            return this;
        }

        public Builder initProgress(InitProgressProfile initProgress) {
            this.initProgress = initProgress;
            return this;
        }

        public Builder totalEntities(Long totalEntities) {
            this.totalEntities = totalEntities;
            return this;
        }

        public Builder activeEntities(Long activeEntities) {
            this.activeEntities = activeEntities;
            return this;
        }

        public Builder adTaskProfile(ADTaskProfile adTaskProfile) {
            this.adTaskProfile = adTaskProfile;
            return this;
        }

        public DetectorProfile build() {
            DetectorProfile profile = new DetectorProfile();
            profile.state = this.state;
            profile.error = this.error;
            profile.modelProfile = modelProfile;
            profile.modelCount = modelCount;
            profile.shingleSize = shingleSize;
            profile.coordinatingNode = coordinatingNode;
            profile.totalSizeInBytes = totalSizeInBytes;
            profile.initProgress = initProgress;
            profile.totalEntities = totalEntities;
            profile.activeEntities = activeEntities;
            profile.adTaskProfile = adTaskProfile;

            return profile;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (state == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeEnum(state);
        }

        out.writeOptionalString(error);
        out.writeOptionalArray(modelProfile);
        out.writeOptionalInt(shingleSize);
        out.writeOptionalString(coordinatingNode);
        out.writeOptionalLong(totalSizeInBytes);
        out.writeOptionalLong(totalEntities);
        out.writeOptionalLong(activeEntities);
        if (initProgress == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            initProgress.writeTo(out);
        }
        if (adTaskProfile == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            adTaskProfile.writeTo(out);
        }
        out.writeVLong(modelCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();

        if (state != null) {
            xContentBuilder.field(CommonName.STATE, state);
        }
        if (error != null) {
            xContentBuilder.field(CommonName.ERROR, error);
        }
        if (modelProfile != null && modelProfile.length > 0) {
            xContentBuilder.startArray(CommonName.MODELS);
            for (ModelProfileOnNode profile : modelProfile) {
                profile.toXContent(xContentBuilder, params);
            }
            xContentBuilder.endArray();
        }
        if (shingleSize != -1) {
            xContentBuilder.field(CommonName.SHINGLE_SIZE, shingleSize);
        }
        if (coordinatingNode != null && !coordinatingNode.isEmpty()) {
            xContentBuilder.field(CommonName.COORDINATING_NODE, coordinatingNode);
        }
        if (totalSizeInBytes != -1) {
            xContentBuilder.field(CommonName.TOTAL_SIZE_IN_BYTES, totalSizeInBytes);
        }
        if (initProgress != null) {
            xContentBuilder.field(CommonName.INIT_PROGRESS, initProgress);
        }
        if (totalEntities != null) {
            xContentBuilder.field(CommonName.TOTAL_ENTITIES, totalEntities);
        }
        if (activeEntities != null) {
            xContentBuilder.field(CommonName.ACTIVE_ENTITIES, activeEntities);
        }
        if (adTaskProfile != null) {
            xContentBuilder.field(CommonName.AD_TASK, adTaskProfile);
        }
        if (modelCount > 0) {
            xContentBuilder.field(CommonName.MODEL_COUNT, modelCount);
        }
        return xContentBuilder.endObject();
    }

    public DetectorState getState() {
        return state;
    }

    public void setState(DetectorState state) {
        this.state = state;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public ModelProfileOnNode[] getModelProfile() {
        return modelProfile;
    }

    public void setModelProfile(ModelProfileOnNode[] modelProfile) {
        this.modelProfile = modelProfile;
    }

    public int getShingleSize() {
        return shingleSize;
    }

    public void setShingleSize(int shingleSize) {
        this.shingleSize = shingleSize;
    }

    public String getCoordinatingNode() {
        return coordinatingNode;
    }

    public void setCoordinatingNode(String coordinatingNode) {
        this.coordinatingNode = coordinatingNode;
    }

    public long getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    public void setTotalSizeInBytes(long totalSizeInBytes) {
        this.totalSizeInBytes = totalSizeInBytes;
    }

    public InitProgressProfile getInitProgress() {
        return initProgress;
    }

    public void setInitProgress(InitProgressProfile initProgress) {
        this.initProgress = initProgress;
    }

    public Long getTotalEntities() {
        return totalEntities;
    }

    public void setTotalEntities(Long totalEntities) {
        this.totalEntities = totalEntities;
    }

    public Long getActiveEntities() {
        return activeEntities;
    }

    public void setActiveEntities(Long activeEntities) {
        this.activeEntities = activeEntities;
    }

    public ADTaskProfile getAdTaskProfile() {
        return adTaskProfile;
    }

    public void setAdTaskProfile(ADTaskProfile adTaskProfile) {
        this.adTaskProfile = adTaskProfile;
    }

    public long getModelCount() {
        return modelCount;
    }

    public void setModelCount(long modelCount) {
        this.modelCount = modelCount;
    }

    @Override
    public void merge(Mergeable other) {
        if (this == other || other == null || getClass() != other.getClass()) {
            return;
        }
        DetectorProfile otherProfile = (DetectorProfile) other;
        if (otherProfile.getState() != null) {
            this.state = otherProfile.getState();
        }
        if (otherProfile.getError() != null) {
            this.error = otherProfile.getError();
        }
        if (otherProfile.getCoordinatingNode() != null) {
            this.coordinatingNode = otherProfile.getCoordinatingNode();
        }
        if (otherProfile.getShingleSize() != -1) {
            this.shingleSize = otherProfile.getShingleSize();
        }
        if (otherProfile.getModelProfile() != null) {
            this.modelProfile = otherProfile.getModelProfile();
        }
        if (otherProfile.getTotalSizeInBytes() != -1) {
            this.totalSizeInBytes = otherProfile.getTotalSizeInBytes();
        }
        if (otherProfile.getInitProgress() != null) {
            this.initProgress = otherProfile.getInitProgress();
        }
        if (otherProfile.getTotalEntities() != null) {
            this.totalEntities = otherProfile.getTotalEntities();
        }
        if (otherProfile.getActiveEntities() != null) {
            this.activeEntities = otherProfile.getActiveEntities();
        }
        if (otherProfile.getAdTaskProfile() != null) {
            this.adTaskProfile = otherProfile.getAdTaskProfile();
        }
        if (otherProfile.getModelCount() > 0) {
            this.modelCount = otherProfile.getModelCount();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        if (obj instanceof DetectorProfile) {
            DetectorProfile other = (DetectorProfile) obj;

            EqualsBuilder equalsBuilder = new EqualsBuilder();
            if (state != null) {
                equalsBuilder.append(state, other.state);
            }
            if (error != null) {
                equalsBuilder.append(error, other.error);
            }
            if (modelProfile != null && modelProfile.length > 0) {
                equalsBuilder.append(modelProfile, other.modelProfile);
            }
            if (shingleSize != -1) {
                equalsBuilder.append(shingleSize, other.shingleSize);
            }
            if (coordinatingNode != null) {
                equalsBuilder.append(coordinatingNode, other.coordinatingNode);
            }
            if (totalSizeInBytes != -1) {
                equalsBuilder.append(totalSizeInBytes, other.totalSizeInBytes);
            }
            if (initProgress != null) {
                equalsBuilder.append(initProgress, other.initProgress);
            }
            if (totalEntities != null) {
                equalsBuilder.append(totalEntities, other.totalEntities);
            }
            if (activeEntities != null) {
                equalsBuilder.append(activeEntities, other.activeEntities);
            }
            if (adTaskProfile != null) {
                equalsBuilder.append(adTaskProfile, other.adTaskProfile);
            }
            if (modelCount > 0) {
                equalsBuilder.append(modelCount, other.modelCount);
            }
            return equalsBuilder.isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
            .append(state)
            .append(error)
            .append(modelProfile)
            .append(shingleSize)
            .append(coordinatingNode)
            .append(totalSizeInBytes)
            .append(initProgress)
            .append(totalEntities)
            .append(activeEntities)
            .append(adTaskProfile)
            .append(modelCount)
            .toHashCode();
    }

    @Override
    public String toString() {
        ToStringBuilder toStringBuilder = new ToStringBuilder(this);

        if (state != null) {
            toStringBuilder.append(CommonName.STATE, state);
        }
        if (error != null) {
            toStringBuilder.append(CommonName.ERROR, error);
        }
        if (modelProfile != null && modelProfile.length > 0) {
            toStringBuilder.append(modelProfile);
        }
        if (shingleSize != -1) {
            toStringBuilder.append(CommonName.SHINGLE_SIZE, shingleSize);
        }
        if (coordinatingNode != null) {
            toStringBuilder.append(CommonName.COORDINATING_NODE, coordinatingNode);
        }
        if (totalSizeInBytes != -1) {
            toStringBuilder.append(CommonName.TOTAL_SIZE_IN_BYTES, totalSizeInBytes);
        }
        if (initProgress != null) {
            toStringBuilder.append(CommonName.INIT_PROGRESS, initProgress);
        }
        if (totalEntities != null) {
            toStringBuilder.append(CommonName.TOTAL_ENTITIES, totalEntities);
        }
        if (activeEntities != null) {
            toStringBuilder.append(CommonName.ACTIVE_ENTITIES, activeEntities);
        }
        if (adTaskProfile != null) {
            toStringBuilder.append(CommonName.AD_TASK, adTaskProfile);
        }
        if (modelCount > 0) {
            toStringBuilder.append(CommonName.MODEL_COUNT, modelCount);
        }
        return toStringBuilder.toString();
    }
}
