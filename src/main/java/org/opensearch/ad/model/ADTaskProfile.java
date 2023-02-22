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

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.opensearch.Version;
import org.opensearch.ad.annotation.Generated;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * One anomaly detection task means one detector starts to run until stopped.
 */
public class ADTaskProfile implements ToXContentObject, Writeable {

    public static final String AD_TASK_FIELD = "ad_task";
    public static final String SHINGLE_SIZE_FIELD = "shingle_size";
    public static final String RCF_TOTAL_UPDATES_FIELD = "rcf_total_updates";
    public static final String THRESHOLD_MODEL_TRAINED_FIELD = "threshold_model_trained";
    public static final String THRESHOLD_MODEL_TRAINING_DATA_SIZE_FIELD = "threshold_model_training_data_size";
    public static final String MODEL_SIZE_IN_BYTES = "model_size_in_bytes";
    public static final String NODE_ID_FIELD = "node_id";
    public static final String TASK_ID_FIELD = "task_id";
    public static final String AD_TASK_TYPE_FIELD = "task_type";
    public static final String DETECTOR_TASK_SLOTS_FIELD = "detector_task_slots";
    public static final String TOTAL_ENTITIES_INITED_FIELD = "total_entities_inited";
    public static final String TOTAL_ENTITIES_COUNT_FIELD = "total_entities_count";
    public static final String PENDING_ENTITIES_COUNT_FIELD = "pending_entities_count";
    public static final String RUNNING_ENTITIES_COUNT_FIELD = "running_entities_count";
    public static final String RUNNING_ENTITIES_FIELD = "running_entities";
    public static final String ENTITY_TASK_PROFILE_FIELD = "entity_task_profiles";
    public static final String LATEST_HC_TASK_RUN_TIME_FIELD = "latest_hc_task_run_time";

    private ADTask adTask;
    private Integer shingleSize;
    private Long rcfTotalUpdates;
    private Boolean thresholdModelTrained;
    private Integer thresholdModelTrainingDataSize;
    private Long modelSizeInBytes;
    private String nodeId;
    private String taskId;
    private String adTaskType;
    private Integer detectorTaskSlots;
    private Boolean totalEntitiesInited;
    private Integer totalEntitiesCount;
    private Integer pendingEntitiesCount;
    private Integer runningEntitiesCount;
    private List<String> runningEntities;
    private Long latestHCTaskRunTime;

    private List<ADEntityTaskProfile> entityTaskProfiles;

    public ADTaskProfile() {

    }

    public ADTaskProfile(ADTask adTask) {
        this.adTask = adTask;
    }

    public ADTaskProfile(
        String taskId,
        int shingleSize,
        long rcfTotalUpdates,
        boolean thresholdModelTrained,
        int thresholdModelTrainingDataSize,
        long modelSizeInBytes,
        String nodeId
    ) {
        this.taskId = taskId;
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
        String taskId,
        String adTaskType,
        Integer detectorTaskSlots,
        Boolean totalEntitiesInited,
        Integer totalEntitiesCount,
        Integer pendingEntitiesCount,
        Integer runningEntitiesCount,
        List<String> runningEntities,
        Long latestHCTaskRunTime
    ) {
        this.adTask = adTask;
        this.shingleSize = shingleSize;
        this.rcfTotalUpdates = rcfTotalUpdates;
        this.thresholdModelTrained = thresholdModelTrained;
        this.thresholdModelTrainingDataSize = thresholdModelTrainingDataSize;
        this.modelSizeInBytes = modelSizeInBytes;
        this.nodeId = nodeId;
        this.taskId = taskId;
        this.adTaskType = adTaskType;
        this.detectorTaskSlots = detectorTaskSlots;
        this.totalEntitiesInited = totalEntitiesInited;
        this.totalEntitiesCount = totalEntitiesCount;
        this.pendingEntitiesCount = pendingEntitiesCount;
        this.runningEntitiesCount = runningEntitiesCount;
        this.runningEntities = runningEntities;
        this.latestHCTaskRunTime = latestHCTaskRunTime;
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
        if (input.available() > 0) {
            this.taskId = input.readOptionalString();
            this.adTaskType = input.readOptionalString();
            this.detectorTaskSlots = input.readOptionalInt();
            this.totalEntitiesInited = input.readOptionalBoolean();
            this.totalEntitiesCount = input.readOptionalInt();
            this.pendingEntitiesCount = input.readOptionalInt();
            this.runningEntitiesCount = input.readOptionalInt();
            if (input.readBoolean()) {
                this.runningEntities = input.readStringList();
            }
            if (input.readBoolean()) {
                this.entityTaskProfiles = input.readList(ADEntityTaskProfile::new);
            }
            this.latestHCTaskRunTime = input.readOptionalLong();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeTo(out, Version.CURRENT);
    }

    public void writeTo(StreamOutput out, Version adVersion) throws IOException {
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
        if (adVersion != null) {
            out.writeOptionalString(taskId);
            out.writeOptionalString(adTaskType);
            out.writeOptionalInt(detectorTaskSlots);
            out.writeOptionalBoolean(totalEntitiesInited);
            out.writeOptionalInt(totalEntitiesCount);
            out.writeOptionalInt(pendingEntitiesCount);
            out.writeOptionalInt(runningEntitiesCount);
            if (runningEntities != null && runningEntities.size() > 0) {
                out.writeBoolean(true);
                out.writeStringCollection(runningEntities);
            } else {
                out.writeBoolean(false);
            }
            if (entityTaskProfiles != null && entityTaskProfiles.size() > 0) {
                out.writeBoolean(true);
                out.writeList(entityTaskProfiles);
            } else {
                out.writeBoolean(false);
            }
            out.writeOptionalLong(latestHCTaskRunTime);
        }
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
        if (taskId != null) {
            xContentBuilder.field(TASK_ID_FIELD, taskId);
        }
        if (adTaskType != null) {
            xContentBuilder.field(AD_TASK_TYPE_FIELD, adTaskType);
        }
        if (detectorTaskSlots != null) {
            xContentBuilder.field(DETECTOR_TASK_SLOTS_FIELD, detectorTaskSlots);
        }
        if (totalEntitiesInited != null) {
            xContentBuilder.field(TOTAL_ENTITIES_INITED_FIELD, totalEntitiesInited);
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
        if (entityTaskProfiles != null && entityTaskProfiles.size() > 0) {
            xContentBuilder.field(ENTITY_TASK_PROFILE_FIELD, entityTaskProfiles.toArray());
        }
        if (latestHCTaskRunTime != null) {
            xContentBuilder.field(LATEST_HC_TASK_RUN_TIME_FIELD, latestHCTaskRunTime);
        }
        return xContentBuilder.endObject();
    }

    public static ADTaskProfile parse(XContentParser parser) throws IOException {
        ADTask adTask = null;
        Integer shingleSize = null;
        Long rcfTotalUpdates = null;
        Boolean thresholdModelTrained = null;
        Integer thresholdModelTrainingDataSize = null;
        Long modelSizeInBytes = null;
        String nodeId = null;
        String taskId = null;
        String taskType = null;
        Integer detectorTaskSlots = null;
        Boolean totalEntitiesInited = null;
        Integer totalEntitiesCount = null;
        Integer pendingEntitiesCount = null;
        Integer runningEntitiesCount = null;
        List<String> runningEntities = null;
        List<ADEntityTaskProfile> entityTaskProfiles = null;
        Long latestHCTaskRunTime = null;

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
                    thresholdModelTrainingDataSize = parser.intValue();
                    break;
                case MODEL_SIZE_IN_BYTES:
                    modelSizeInBytes = parser.longValue();
                    break;
                case NODE_ID_FIELD:
                    nodeId = parser.text();
                    break;
                case TASK_ID_FIELD:
                    taskId = parser.text();
                    break;
                case AD_TASK_TYPE_FIELD:
                    taskType = parser.text();
                    break;
                case DETECTOR_TASK_SLOTS_FIELD:
                    detectorTaskSlots = parser.intValue();
                    break;
                case TOTAL_ENTITIES_INITED_FIELD:
                    totalEntitiesInited = parser.booleanValue();
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
                case ENTITY_TASK_PROFILE_FIELD:
                    entityTaskProfiles = new ArrayList<>();
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        entityTaskProfiles.add(ADEntityTaskProfile.parse(parser));
                    }
                    break;
                case LATEST_HC_TASK_RUN_TIME_FIELD:
                    latestHCTaskRunTime = parser.longValue();
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
            thresholdModelTrainingDataSize,
            modelSizeInBytes,
            nodeId,
            taskId,
            taskType,
            detectorTaskSlots,
            totalEntitiesInited,
            totalEntitiesCount,
            pendingEntitiesCount,
            runningEntitiesCount,
            runningEntities,
            latestHCTaskRunTime
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

    public boolean getTotalEntitiesInited() {
        return totalEntitiesInited != null && totalEntitiesInited.booleanValue();
    }

    public void setTotalEntitiesInited(Boolean totalEntitiesInited) {
        this.totalEntitiesInited = totalEntitiesInited;
    }

    public Long getLatestHCTaskRunTime() {
        return latestHCTaskRunTime;
    }

    public void setLatestHCTaskRunTime(Long latestHCTaskRunTime) {
        this.latestHCTaskRunTime = latestHCTaskRunTime;
    }

    public Integer getTotalEntitiesCount() {
        return totalEntitiesCount;
    }

    public void setTotalEntitiesCount(Integer totalEntitiesCount) {
        this.totalEntitiesCount = totalEntitiesCount;
    }

    public Integer getDetectorTaskSlots() {
        return detectorTaskSlots;
    }

    public void setDetectorTaskSlots(Integer detectorTaskSlots) {
        this.detectorTaskSlots = detectorTaskSlots;
    }

    public Integer getPendingEntitiesCount() {
        return pendingEntitiesCount;
    }

    public void setPendingEntitiesCount(Integer pendingEntitiesCount) {
        this.pendingEntitiesCount = pendingEntitiesCount;
    }

    public Integer getRunningEntitiesCount() {
        return runningEntitiesCount;
    }

    public void setRunningEntitiesCount(Integer runningEntitiesCount) {
        this.runningEntitiesCount = runningEntitiesCount;
    }

    public List<String> getRunningEntities() {
        return runningEntities;
    }

    public void setRunningEntities(List<String> runningEntities) {
        this.runningEntities = runningEntities;
    }

    public List<ADEntityTaskProfile> getEntityTaskProfiles() {
        return entityTaskProfiles;
    }

    public void setEntityTaskProfiles(List<ADEntityTaskProfile> entityTaskProfiles) {
        this.entityTaskProfiles = entityTaskProfiles;
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ADTaskProfile that = (ADTaskProfile) o;
        return Objects.equals(adTask, that.adTask)
            && Objects.equals(shingleSize, that.shingleSize)
            && Objects.equals(rcfTotalUpdates, that.rcfTotalUpdates)
            && Objects.equals(thresholdModelTrained, that.thresholdModelTrained)
            && Objects.equals(thresholdModelTrainingDataSize, that.thresholdModelTrainingDataSize)
            && Objects.equals(modelSizeInBytes, that.modelSizeInBytes)
            && Objects.equals(nodeId, that.nodeId)
            && Objects.equals(taskId, that.taskId)
            && Objects.equals(adTaskType, that.adTaskType)
            && Objects.equals(detectorTaskSlots, that.detectorTaskSlots)
            && Objects.equals(totalEntitiesInited, that.totalEntitiesInited)
            && Objects.equals(totalEntitiesCount, that.totalEntitiesCount)
            && Objects.equals(pendingEntitiesCount, that.pendingEntitiesCount)
            && Objects.equals(runningEntitiesCount, that.runningEntitiesCount)
            && Objects.equals(runningEntities, that.runningEntities)
            && Objects.equals(latestHCTaskRunTime, that.latestHCTaskRunTime)
            && Objects.equals(entityTaskProfiles, that.entityTaskProfiles);
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects
            .hash(
                adTask,
                shingleSize,
                rcfTotalUpdates,
                thresholdModelTrained,
                thresholdModelTrainingDataSize,
                modelSizeInBytes,
                nodeId,
                taskId,
                adTaskType,
                detectorTaskSlots,
                totalEntitiesInited,
                totalEntitiesCount,
                pendingEntitiesCount,
                runningEntitiesCount,
                runningEntities,
                entityTaskProfiles,
                latestHCTaskRunTime
            );
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
            + ", taskId='"
            + taskId
            + '\''
            + ", adTaskType='"
            + adTaskType
            + '\''
            + ", detectorTaskSlots="
            + detectorTaskSlots
            + ", totalEntitiesInited="
            + totalEntitiesInited
            + ", totalEntitiesCount="
            + totalEntitiesCount
            + ", pendingEntitiesCount="
            + pendingEntitiesCount
            + ", runningEntitiesCount="
            + runningEntitiesCount
            + ", runningEntities="
            + runningEntities
            + ", latestHCTaskRunTime="
            + latestHCTaskRunTime
            + ", entityTaskProfiles="
            + entityTaskProfiles
            + '}';
    }
}
