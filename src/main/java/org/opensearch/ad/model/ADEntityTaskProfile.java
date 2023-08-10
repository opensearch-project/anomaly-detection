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

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.Objects;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * HC detector's entity task profile.
 */
public class ADEntityTaskProfile implements ToXContentObject, Writeable {

    public static final String SHINGLE_SIZE_FIELD = "shingle_size";
    public static final String RCF_TOTAL_UPDATES_FIELD = "rcf_total_updates";
    public static final String THRESHOLD_MODEL_TRAINED_FIELD = "threshold_model_trained";
    public static final String THRESHOLD_MODEL_TRAINING_DATA_SIZE_FIELD = "threshold_model_training_data_size";
    public static final String MODEL_SIZE_IN_BYTES = "model_size_in_bytes";
    public static final String NODE_ID_FIELD = "node_id";
    public static final String ENTITY_FIELD = "entity";
    public static final String TASK_ID_FIELD = "task_id";
    public static final String AD_TASK_TYPE_FIELD = "task_type";

    private Integer shingleSize;
    private Long rcfTotalUpdates;
    private Boolean thresholdModelTrained;
    private Integer thresholdModelTrainingDataSize;
    private Long modelSizeInBytes;
    private String nodeId;
    private Entity entity;
    private String taskId;
    private String adTaskType;

    public ADEntityTaskProfile(
        Integer shingleSize,
        Long rcfTotalUpdates,
        Boolean thresholdModelTrained,
        Integer thresholdModelTrainingDataSize,
        Long modelSizeInBytes,
        String nodeId,
        Entity entity,
        String taskId,
        String adTaskType
    ) {
        this.shingleSize = shingleSize;
        this.rcfTotalUpdates = rcfTotalUpdates;
        this.thresholdModelTrained = thresholdModelTrained;
        this.thresholdModelTrainingDataSize = thresholdModelTrainingDataSize;
        this.modelSizeInBytes = modelSizeInBytes;
        this.nodeId = nodeId;
        this.entity = entity;
        this.taskId = taskId;
        this.adTaskType = adTaskType;
    }

    public static ADEntityTaskProfile parse(XContentParser parser) throws IOException {
        Integer shingleSize = null;
        Long rcfTotalUpdates = null;
        Boolean thresholdModelTrained = null;
        Integer thresholdModelTrainingDataSize = null;
        Long modelSizeInBytes = null;
        String nodeId = null;
        Entity entity = null;
        String taskId = null;
        String taskType = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case SHINGLE_SIZE_FIELD:
                    shingleSize = parser.intValue();
                    break;
                case RCF_TOTAL_UPDATES_FIELD:
                    rcfTotalUpdates = parser.longValue();
                    break;
                case THRESHOLD_MODEL_TRAINED_FIELD:
                    thresholdModelTrained = parser.booleanValue();
                    break;
                case THRESHOLD_MODEL_TRAINING_DATA_SIZE_FIELD:
                    thresholdModelTrainingDataSize = parser.intValue();
                    break;
                case MODEL_SIZE_IN_BYTES:
                    modelSizeInBytes = parser.longValue();
                    break;
                case NODE_ID_FIELD:
                    nodeId = parser.text();
                    break;
                case ENTITY_FIELD:
                    entity = Entity.parse(parser);
                    break;
                case TASK_ID_FIELD:
                    taskId = parser.text();
                    break;
                case AD_TASK_TYPE_FIELD:
                    taskType = parser.text();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new ADEntityTaskProfile(
            shingleSize,
            rcfTotalUpdates,
            thresholdModelTrained,
            thresholdModelTrainingDataSize,
            modelSizeInBytes,
            nodeId,
            entity,
            taskId,
            taskType
        );
    }

    public ADEntityTaskProfile(StreamInput input) throws IOException {
        this.shingleSize = input.readOptionalInt();
        this.rcfTotalUpdates = input.readOptionalLong();
        this.thresholdModelTrained = input.readOptionalBoolean();
        this.thresholdModelTrainingDataSize = input.readOptionalInt();
        this.modelSizeInBytes = input.readOptionalLong();
        this.nodeId = input.readOptionalString();
        if (input.readBoolean()) {
            this.entity = new Entity(input);
        } else {
            this.entity = null;
        }
        this.taskId = input.readOptionalString();
        this.adTaskType = input.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(shingleSize);
        out.writeOptionalLong(rcfTotalUpdates);
        out.writeOptionalBoolean(thresholdModelTrained);
        out.writeOptionalInt(thresholdModelTrainingDataSize);
        out.writeOptionalLong(modelSizeInBytes);
        out.writeOptionalString(nodeId);
        if (entity != null) {
            out.writeBoolean(true);
            entity.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalString(taskId);
        out.writeOptionalString(adTaskType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        if (shingleSize != null) {
            xContentBuilder.field(SHINGLE_SIZE_FIELD, shingleSize);
        }
        if (rcfTotalUpdates != null) {
            xContentBuilder.field(RCF_TOTAL_UPDATES_FIELD, rcfTotalUpdates);
        }
        if (thresholdModelTrained != null) {
            xContentBuilder.field(THRESHOLD_MODEL_TRAINED_FIELD, thresholdModelTrained);
        }
        if (thresholdModelTrainingDataSize != null) {
            xContentBuilder.field(THRESHOLD_MODEL_TRAINING_DATA_SIZE_FIELD, thresholdModelTrainingDataSize);
        }
        if (modelSizeInBytes != null) {
            xContentBuilder.field(MODEL_SIZE_IN_BYTES, modelSizeInBytes);
        }
        if (nodeId != null) {
            xContentBuilder.field(NODE_ID_FIELD, nodeId);
        }
        if (entity != null) {
            xContentBuilder.field(ENTITY_FIELD, entity);
        }
        if (taskId != null) {
            xContentBuilder.field(TASK_ID_FIELD, taskId);
        }
        if (adTaskType != null) {
            xContentBuilder.field(AD_TASK_TYPE_FIELD, adTaskType);
        }
        return xContentBuilder.endObject();
    }

    public Integer getShingleSize() {
        return shingleSize;
    }

    public void setShingleSize(Integer shingleSize) {
        this.shingleSize = shingleSize;
    }

    public Long getRcfTotalUpdates() {
        return rcfTotalUpdates;
    }

    public void setRcfTotalUpdates(Long rcfTotalUpdates) {
        this.rcfTotalUpdates = rcfTotalUpdates;
    }

    public Boolean getThresholdModelTrained() {
        return thresholdModelTrained;
    }

    public void setThresholdModelTrained(Boolean thresholdModelTrained) {
        this.thresholdModelTrained = thresholdModelTrained;
    }

    public Integer getThresholdModelTrainingDataSize() {
        return thresholdModelTrainingDataSize;
    }

    public void setThresholdModelTrainingDataSize(Integer thresholdModelTrainingDataSize) {
        this.thresholdModelTrainingDataSize = thresholdModelTrainingDataSize;
    }

    public Long getModelSizeInBytes() {
        return modelSizeInBytes;
    }

    public void setModelSizeInBytes(Long modelSizeInBytes) {
        this.modelSizeInBytes = modelSizeInBytes;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public Entity getEntity() {
        return entity;
    }

    public void setEntity(Entity entity) {
        this.entity = entity;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getAdTaskType() {
        return adTaskType;
    }

    public void setAdTaskType(String adTaskType) {
        this.adTaskType = adTaskType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ADEntityTaskProfile that = (ADEntityTaskProfile) o;
        return Objects.equals(shingleSize, that.shingleSize)
            && Objects.equals(rcfTotalUpdates, that.rcfTotalUpdates)
            && Objects.equals(thresholdModelTrained, that.thresholdModelTrained)
            && Objects.equals(thresholdModelTrainingDataSize, that.thresholdModelTrainingDataSize)
            && Objects.equals(modelSizeInBytes, that.modelSizeInBytes)
            && Objects.equals(nodeId, that.nodeId)
            && Objects.equals(taskId, that.taskId)
            && Objects.equals(adTaskType, that.adTaskType)
            && Objects.equals(entity, that.entity);
    }

    @Override
    public int hashCode() {
        return Objects
            .hash(
                shingleSize,
                rcfTotalUpdates,
                thresholdModelTrained,
                thresholdModelTrainingDataSize,
                modelSizeInBytes,
                nodeId,
                entity,
                taskId,
                adTaskType
            );
    }
}
