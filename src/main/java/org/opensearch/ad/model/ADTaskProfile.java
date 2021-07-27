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

package org.opensearch.ad.model;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.opensearch.ad.annotation.Generated;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import com.google.common.base.Objects;

/**
 * One anomaly detection task means one detector starts to run until stopped.
 */
public class ADTaskProfile implements ToXContentObject, Writeable, Writeable.Writer, Writeable.Reader {

    public static final String AD_TASK_FIELD = "ad_task";
    public static final String SHINGLE_SIZE_FIELD = "shingle_size";
    public static final String RCF_TOTAL_UPDATES_FIELD = "rcf_total_updates";
    public static final String THRESHOLD_MODEL_TRAINED_FIELD = "threshold_model_trained";
    public static final String THRESHOLD_MODEL_TRAINING_DATA_SIZE_FIELD = "threshold_model_training_data_size";
    public static final String MODEL_SIZE_IN_BYTES = "model_size_in_bytes";
    public static final String NODE_ID_FIELD = "node_id";
    public static final String ENTITY_FIELD = "entity";
    public static final String TASK_ID_FIELD = "task_id";
    public static final String AD_TASK_TYPE_FIELD = "task_type";
    public static final String TOTAL_ENTITIES_COUNT_FIELD = "total_entities_count";
    public static final String PENDING_ENTITIES_COUNT_FIELD = "pending_entities_count";
    public static final String RUNNING_ENTITIES_COUNT_FIELD = "running_entities_count";
    public static final String RUNNING_ENTITIES_FIELD = "running_entities";

    private ADTask adTask;
    private Integer shingleSize;
    private Long rcfTotalUpdates;
    private Boolean thresholdModelTrained;
    private Integer thresholdModelTrainingDataSize;
    private Long modelSizeInBytes;
    private String nodeId;
    private Entity entity;
    private String taskId;
    private String adTaskType;
    private Integer totalEntitiesCount;
    private Integer pendingEntitiesCount;
    private Integer runningEntitiesCount;
    private String[] runningEntities;

    public ADTaskProfile(
        Integer shingleSize,
        Long rcfTotalUpdates,
        Boolean thresholdModelTrained,
        Integer thresholdModelTrainingDataSize,
        Long modelSizeInBytes,
        String nodeId
    ) {
        this(null, shingleSize, rcfTotalUpdates, thresholdModelTrained, thresholdModelTrainingDataSize, modelSizeInBytes, nodeId);
    }

    public ADTaskProfile(
        String nodeId,
        Integer totalEntitiesCount,
        Integer pendingEntitiesCount,
        Integer runningEntitiesCount,
        String[] runningEntities
    ) {
        this(
            null,
            null,
            null,
            null,
            null,
            null,
            nodeId,
            null,
            null,
            totalEntitiesCount,
            pendingEntitiesCount,
            runningEntitiesCount,
            runningEntities
        );
    }

    public ADTaskProfile(
        Integer shingleSize,
        Long rcfTotalUpdates,
        Boolean thresholdModelTrained,
        Integer thresholdModelTrainingDataSize,
        Long modelSizeInBytes,
        String nodeId,
        Entity entity,
        String taskId
    ) {
        this(
            null,
            shingleSize,
            rcfTotalUpdates,
            thresholdModelTrained,
            thresholdModelTrainingDataSize,
            modelSizeInBytes,
            nodeId,
            entity,
            taskId,
            null,
            null,
            null,
            null
        );
    }

    public ADTaskProfile(
        ADTask adTask,
        Integer shingleSize,
        Long rcfTotalUpdates,
        Boolean thresholdModelTrained,
        Integer thresholdModelTrainingDataSize,
        Long modelSizeInBytes,
        String nodeId
    ) {
        this.adTask = adTask;
        this.shingleSize = shingleSize;
        this.rcfTotalUpdates = rcfTotalUpdates;
        this.thresholdModelTrained = thresholdModelTrained;
        this.thresholdModelTrainingDataSize = thresholdModelTrainingDataSize;
        this.modelSizeInBytes = modelSizeInBytes;
        this.nodeId = nodeId;
    }

    public ADTaskProfile(
        ADTask adTask,
        Integer shingleSize,
        Long rcfTotalUpdates,
        Boolean thresholdModelTrained,
        Integer thresholdModelTrainingDataSize,
        Long modelSizeInBytes,
        String nodeId,
        Entity entity,
        String taskId,
        Integer totalEntitiesCount,
        Integer pendingEntitiesCount,
        Integer runningEntitiesCount,
        String[] runningEntities
    ) {
        this.adTask = adTask;
        this.shingleSize = shingleSize;
        this.rcfTotalUpdates = rcfTotalUpdates;
        this.thresholdModelTrained = thresholdModelTrained;
        this.thresholdModelTrainingDataSize = thresholdModelTrainingDataSize;
        this.modelSizeInBytes = modelSizeInBytes;
        this.nodeId = nodeId;
        this.entity = entity;
        this.taskId = taskId;
        this.totalEntitiesCount = totalEntitiesCount;
        this.pendingEntitiesCount = pendingEntitiesCount;
        this.runningEntitiesCount = runningEntitiesCount;
        this.runningEntities = runningEntities;
        if (entity != null && entity.getAttributes().size() > 0) {
            setAdTaskType(ADTaskType.HISTORICAL_HC_ENTITY.name());
        } else if (this.pendingEntitiesCount != null || runningEntities != null) {
            setAdTaskType(ADTaskType.HISTORICAL_HC_DETECTOR.name());
        }
    }

    public String getAdTaskType() {
        return adTaskType;
    }

    public ADTaskProfile(StreamInput input) throws IOException {
        if (input.readBoolean()) {
            this.adTask = new ADTask(input);
        } else {
            this.adTask = null;
        }
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
        totalEntitiesCount = input.readOptionalInt();
        pendingEntitiesCount = input.readOptionalInt();
        runningEntitiesCount = input.readOptionalInt();
        runningEntities = input.readOptionalStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (adTask != null) {
            out.writeBoolean(true);
            adTask.writeTo(out);
        } else {
            out.writeBoolean(false);
        }

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
        out.writeOptionalInt(totalEntitiesCount);
        out.writeOptionalInt(pendingEntitiesCount);
        out.writeOptionalInt(runningEntitiesCount);
        out.writeOptionalStringArray(runningEntities);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        if (adTask != null) {
            xContentBuilder.field(AD_TASK_FIELD, adTask);
        }
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
        if (adTaskType != null) {
            xContentBuilder.field(AD_TASK_TYPE_FIELD, adTaskType);
        }
        if (totalEntitiesCount != null) {
            xContentBuilder.field(TOTAL_ENTITIES_COUNT_FIELD, totalEntitiesCount);
        }
        if (pendingEntitiesCount != null) {
            xContentBuilder.field(PENDING_ENTITIES_COUNT_FIELD, pendingEntitiesCount);
        }
        if (runningEntitiesCount != null) {
            xContentBuilder.field(RUNNING_ENTITIES_COUNT_FIELD, runningEntitiesCount);
        }
        if (runningEntities != null) {
            xContentBuilder.field(RUNNING_ENTITIES_FIELD, runningEntities);
        }
        return xContentBuilder.endObject();
    }

    public static ADTaskProfile parse(XContentParser parser) throws IOException {
        ADTask adTask = null;
        Integer shingleSize = null;
        Long rcfTotalUpdates = null;
        Boolean thresholdModelTrained = null;
        Integer thresholdNodelTrainingDataSize = null;
        Long modelSizeInBytes = null;
        String nodeId = null;
        Entity entity = null;
        String taskId = null;
        Integer totalEntitiesCount = null;
        Integer pendingEntitiesCount = null;
        Integer runningEntitiesCount = null;
        List<String> runningEntities = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case AD_TASK_FIELD:
                    adTask = ADTask.parse(parser);
                    break;
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
                    thresholdNodelTrainingDataSize = parser.intValue();
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
                case TOTAL_ENTITIES_COUNT_FIELD:
                    totalEntitiesCount = parser.intValue();
                    break;
                case PENDING_ENTITIES_COUNT_FIELD:
                    pendingEntitiesCount = parser.intValue();
                    break;
                case RUNNING_ENTITIES_COUNT_FIELD:
                    runningEntitiesCount = parser.intValue();
                    break;
                case RUNNING_ENTITIES_FIELD:
                    runningEntities = new ArrayList<>();
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        runningEntities.add(parser.text());
                    }
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new ADTaskProfile(
            adTask,
            shingleSize,
            rcfTotalUpdates,
            thresholdModelTrained,
            thresholdNodelTrainingDataSize,
            modelSizeInBytes,
            nodeId,
            entity,
            taskId,
            totalEntitiesCount,
            pendingEntitiesCount,
            runningEntitiesCount,
            runningEntities == null ? null : runningEntities.toArray(new String[0])
        );
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ADTaskProfile that = (ADTaskProfile) o;
        return Objects.equal(getAdTask(), that.getAdTask())
            && Objects.equal(getShingleSize(), that.getShingleSize())
            && Objects.equal(getRcfTotalUpdates(), that.getRcfTotalUpdates())
            && Objects.equal(getThresholdModelTrained(), that.getThresholdModelTrained())
            && Objects.equal(getModelSizeInBytes(), that.getModelSizeInBytes())
            && Objects.equal(getNodeId(), that.getNodeId())
            && Objects.equal(getThresholdModelTrainingDataSize(), that.getThresholdModelTrainingDataSize());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects
            .hashCode(
                adTask,
                shingleSize,
                rcfTotalUpdates,
                thresholdModelTrained,
                thresholdModelTrainingDataSize,
                modelSizeInBytes,
                nodeId
            );
    }

    public ADTask getAdTask() {
        return adTask;
    }

    public void setAdTask(ADTask adTask) {
        this.adTask = adTask;
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

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public Long getModelSizeInBytes() {
        return modelSizeInBytes;
    }

    public void setModelSizeInBytes(Long modelSizeInBytes) {
        this.modelSizeInBytes = modelSizeInBytes;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public void setAdTaskType(String adTaskType) {
        this.adTaskType = adTaskType;
    }

    public Entity getEntity() {
        return entity;
    }

    public Integer getTotalEntitiesCount() {
        return totalEntitiesCount;
    }

    public Integer getPendingEntitiesCount() {
        return pendingEntitiesCount;
    }

    public Integer getRunningEntitiesCount() {
        return runningEntitiesCount;
    }

    public String[] getRunningEntities() {
        return runningEntities;
    }

    @Override
    public String toString() {
        return "ADTaskProfile{"
            + "adTask="
            + adTask
            + ", shingleSize="
            + shingleSize
            + ", rcfTotalUpdates="
            + rcfTotalUpdates
            + ", thresholdModelTrained="
            + thresholdModelTrained
            + ", thresholdModelTrainingDataSize="
            + thresholdModelTrainingDataSize
            + ", modelSizeInBytes="
            + modelSizeInBytes
            + ", nodeId='"
            + nodeId
            + '\''
            + ", entity="
            + entity
            + ", taskId='"
            + taskId
            + '\''
            + ", adTaskType='"
            + adTaskType
            + '\''
            + ", totalEntitiesCount="
            + totalEntitiesCount
            + ", pendingEntitiesCount="
            + pendingEntitiesCount
            + ", runningEntitiesCount="
            + runningEntitiesCount
            + ", runningEntities="
            + Arrays.toString(runningEntities)
            + '}';
    }

    @Override
    public Object read(StreamInput in) throws IOException {
        return new ADTaskProfile(in);
    }

    @Override
    public void write(StreamOutput out, Object value) throws IOException {
        if (value instanceof ADTaskProfile) {
            ((ADTaskProfile) value).writeTo(out);
        } else {
            throw new IllegalArgumentException("Can only write ADTaskProfile");
        }
    }
}
