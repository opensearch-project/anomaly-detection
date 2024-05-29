/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.model;

import java.io.IOException;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.TaskProfile;
import org.opensearch.timeseries.constant.CommonName;

public abstract class ConfigProfile<TaskClass extends TimeSeriesTask, TaskProfileType extends TaskProfile<TaskClass>>
    implements
        Writeable,
        ToXContentObject,
        Mergeable {

    protected ConfigState state;
    protected String error;
    protected ModelProfileOnNode[] modelProfile;
    protected String coordinatingNode;
    protected long totalSizeInBytes;
    protected InitProgressProfile initProgress;
    protected Long totalEntities;
    protected Long activeEntities;
    protected TaskProfileType taskProfile;
    protected long modelCount;
    protected String taskName;

    public ConfigProfile(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            this.state = in.readEnum(ConfigState.class);
        }

        this.error = in.readOptionalString();
        this.modelProfile = in.readOptionalArray(ModelProfileOnNode::new, ModelProfileOnNode[]::new);
        this.coordinatingNode = in.readOptionalString();
        this.totalSizeInBytes = in.readOptionalLong();
        this.totalEntities = in.readOptionalLong();
        this.activeEntities = in.readOptionalLong();
        if (in.readBoolean()) {
            this.initProgress = new InitProgressProfile(in);
        }
        if (in.readBoolean()) {
            this.taskProfile = createTaskProfile(in);
        }
        this.modelCount = in.readVLong();
    }

    protected ConfigProfile() {

    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    public static abstract class Builder<TaskClass extends TimeSeriesTask, TaskProfileType extends TaskProfile<TaskClass>> {
        protected ConfigState state = null;
        protected String error = null;
        protected ModelProfileOnNode[] modelProfile = null;
        protected String coordinatingNode = null;
        protected long totalSizeInBytes = -1;
        protected InitProgressProfile initProgress = null;
        protected Long totalEntities;
        protected Long activeEntities;
        protected long modelCount = 0;

        public Builder() {}

        public Builder<TaskClass, TaskProfileType> state(ConfigState state) {
            this.state = state;
            return this;
        }

        public Builder<TaskClass, TaskProfileType> error(String error) {
            this.error = error;
            return this;
        }

        public Builder<TaskClass, TaskProfileType> modelProfile(ModelProfileOnNode[] modelProfile) {
            this.modelProfile = modelProfile;
            return this;
        }

        public Builder<TaskClass, TaskProfileType> modelCount(long modelCount) {
            this.modelCount = modelCount;
            return this;
        }

        public Builder<TaskClass, TaskProfileType> coordinatingNode(String coordinatingNode) {
            this.coordinatingNode = coordinatingNode;
            return this;
        }

        public Builder<TaskClass, TaskProfileType> totalSizeInBytes(long totalSizeInBytes) {
            this.totalSizeInBytes = totalSizeInBytes;
            return this;
        }

        public Builder<TaskClass, TaskProfileType> initProgress(InitProgressProfile initProgress) {
            this.initProgress = initProgress;
            return this;
        }

        public Builder<TaskClass, TaskProfileType> totalEntities(Long totalEntities) {
            this.totalEntities = totalEntities;
            return this;
        }

        public Builder<TaskClass, TaskProfileType> activeEntities(Long activeEntities) {
            this.activeEntities = activeEntities;
            return this;
        }

        public abstract Builder<TaskClass, TaskProfileType> taskProfile(TaskProfileType taskProfile);

        public abstract <ConfigProfileType extends ConfigProfile<TaskClass, TaskProfileType>> ConfigProfileType build();
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
        if (taskProfile == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            taskProfile.writeTo(out);
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
        if (taskProfile != null) {
            xContentBuilder.field(getTaskFieldName(), taskProfile);
        }
        if (modelCount > 0) {
            xContentBuilder.field(CommonName.MODEL_COUNT, modelCount);
        }
        return xContentBuilder.endObject();
    }

    public ConfigState getState() {
        return state;
    }

    public void setState(ConfigState state) {
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

    public TaskProfileType getTaskProfile() {
        return taskProfile;
    }

    public void setTaskProfile(TaskProfileType taskProfile) {
        this.taskProfile = taskProfile;
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
        ConfigProfile<TaskClass, TaskProfileType> otherProfile = (ConfigProfile<TaskClass, TaskProfileType>) other;
        if (otherProfile.getState() != null) {
            this.state = otherProfile.getState();
        }
        if (otherProfile.getError() != null) {
            this.error = otherProfile.getError();
        }
        if (otherProfile.getCoordinatingNode() != null) {
            this.coordinatingNode = otherProfile.getCoordinatingNode();
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
        if (otherProfile.getTaskProfile() != null) {
            this.taskProfile = otherProfile.getTaskProfile();
        }
        if (otherProfile.getModelCount() > 0) {
            this.modelCount = otherProfile.getModelCount();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        if (obj instanceof ConfigProfile) {
            ConfigProfile<TaskClass, TaskProfileType> other = (ConfigProfile<TaskClass, TaskProfileType>) obj;

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
            if (taskProfile != null) {
                equalsBuilder.append(taskProfile, other.taskProfile);
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
            .append(coordinatingNode)
            .append(totalSizeInBytes)
            .append(initProgress)
            .append(totalEntities)
            .append(activeEntities)
            .append(taskProfile)
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
        if (taskProfile != null) {
            toStringBuilder.append(getTaskFieldName(), taskProfile);
        }
        if (modelCount > 0) {
            toStringBuilder.append(CommonName.MODEL_COUNT, modelCount);
        }
        return toStringBuilder.toString();
    }

    protected abstract TaskProfileType createTaskProfile(StreamInput in) throws IOException;

    protected abstract String getTaskFieldName();
}
