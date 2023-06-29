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

package org.opensearch.timeseries.model;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.timeseries.annotation.Generated;

import com.google.common.base.Objects;

public abstract class IndexableResult implements Writeable, ToXContentObject {

    protected final String configId;
    protected final List<FeatureData> featureData;
    protected final Instant dataStartTime;
    protected final Instant dataEndTime;
    protected final Instant executionStartTime;
    protected final Instant executionEndTime;
    protected final String error;
    protected final Optional<Entity> optionalEntity;
    protected User user;
    protected final Integer schemaVersion;
    /*
     * model id for easy aggregations of entities. The front end needs to query
     * for entities ordered by the descending/ascending order of feature values.
     * After supporting multi-category fields, it is hard to write such queries
     * since the entity information is stored in a nested object array.
     * Also, the front end has all code/queries/ helper functions in place to
     * rely on a single key per entity combo. Adding model id to forecast result
     * to help the transition to multi-categorical field less painful.
     */
    protected final String modelId;
    protected final String entityId;
    protected final String taskId;

    public IndexableResult(
        String configId,
        List<FeatureData> featureData,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executionStartTime,
        Instant executionEndTime,
        String error,
        Optional<Entity> entity,
        User user,
        Integer schemaVersion,
        String modelId,
        String taskId
    ) {
        this.configId = configId;
        this.featureData = featureData;
        this.dataStartTime = dataStartTime;
        this.dataEndTime = dataEndTime;
        this.executionStartTime = executionStartTime;
        this.executionEndTime = executionEndTime;
        this.error = error;
        this.optionalEntity = entity;
        this.user = user;
        this.schemaVersion = schemaVersion;
        this.modelId = modelId;
        this.taskId = taskId;
        this.entityId = getEntityId(entity, configId);
    }

    public IndexableResult(StreamInput input) throws IOException {
        this.configId = input.readString();
        int featureSize = input.readVInt();
        this.featureData = new ArrayList<>(featureSize);
        for (int i = 0; i < featureSize; i++) {
            featureData.add(new FeatureData(input));
        }
        this.dataStartTime = input.readInstant();
        this.dataEndTime = input.readInstant();
        this.executionStartTime = input.readInstant();
        this.executionEndTime = input.readInstant();
        this.error = input.readOptionalString();
        if (input.readBoolean()) {
            this.optionalEntity = Optional.of(new Entity(input));
        } else {
            this.optionalEntity = Optional.empty();
        }
        if (input.readBoolean()) {
            this.user = new User(input);
        } else {
            user = null;
        }
        this.schemaVersion = input.readInt();
        this.modelId = input.readOptionalString();
        this.taskId = input.readOptionalString();
        this.entityId = input.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(configId);
        out.writeVInt(featureData.size());
        for (FeatureData feature : featureData) {
            feature.writeTo(out);
        }
        out.writeInstant(dataStartTime);
        out.writeInstant(dataEndTime);
        out.writeInstant(executionStartTime);
        out.writeInstant(executionEndTime);
        out.writeOptionalString(error);
        if (optionalEntity.isPresent()) {
            out.writeBoolean(true);
            optionalEntity.get().writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        if (user != null) {
            out.writeBoolean(true); // user exists
            user.writeTo(out);
        } else {
            out.writeBoolean(false); // user does not exist
        }
        out.writeInt(schemaVersion);
        out.writeOptionalString(modelId);
        out.writeOptionalString(taskId);
        out.writeOptionalString(entityId);
    }

    public String getConfigId() {
        return configId;
    }

    public List<FeatureData> getFeatureData() {
        return featureData;
    }

    public Instant getDataStartTime() {
        return dataStartTime;
    }

    public Instant getDataEndTime() {
        return dataEndTime;
    }

    public Instant getExecutionStartTime() {
        return executionStartTime;
    }

    public Instant getExecutionEndTime() {
        return executionEndTime;
    }

    public String getError() {
        return error;
    }

    public Optional<Entity> getEntity() {
        return optionalEntity;
    }

    public String getModelId() {
        return modelId;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getEntityId() {
        return entityId;
    }

    /**
     * entityId equals to model Id. It is hard to explain to users what
     * modelId is. entityId is more user friendly.
     * @param entity Entity info
     * @param configId config id
     * @return entity id
     */
    public static String getEntityId(Optional<Entity> entity, String configId) {
        return entity.flatMap(e -> e.getModelId(configId)).orElse(null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        IndexableResult that = (IndexableResult) o;
        return Objects.equal(configId, that.configId)
            && Objects.equal(taskId, that.taskId)
            && Objects.equal(featureData, that.featureData)
            && Objects.equal(dataStartTime, that.dataStartTime)
            && Objects.equal(dataEndTime, that.dataEndTime)
            && Objects.equal(executionStartTime, that.executionStartTime)
            && Objects.equal(executionEndTime, that.executionEndTime)
            && Objects.equal(error, that.error)
            && Objects.equal(optionalEntity, that.optionalEntity)
            && Objects.equal(modelId, that.modelId)
            && Objects.equal(entityId, that.entityId);
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects
            .hashCode(
                configId,
                taskId,
                featureData,
                dataStartTime,
                dataEndTime,
                executionStartTime,
                executionEndTime,
                error,
                optionalEntity,
                modelId,
                entityId
            );
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("configId", configId)
            .append("taskId", taskId)
            .append("featureData", featureData)
            .append("dataStartTime", dataStartTime)
            .append("dataEndTime", dataEndTime)
            .append("executionStartTime", executionStartTime)
            .append("executionEndTime", executionEndTime)
            .append("error", error)
            .append("entity", optionalEntity)
            .append("modelId", modelId)
            .append("entityId", entityId)
            .toString();
    }

    /**
     * Used to throw away requests when index pressure is high.
     * @return  whether the result is high priority.
     */
    public abstract boolean isHighPriority();
}
