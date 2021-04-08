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

package com.amazon.opendistroforelasticsearch.ad.model;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import com.amazon.opendistroforelasticsearch.ad.annotation.Generated;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.google.common.base.Objects;

/**
 * One anomaly detection task means one detector starts to run until stopped.
 */
public class ADTask implements ToXContentObject, Writeable {

    public static final String TASK_ID_FIELD = "task_id";
    public static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    public static final String STARTED_BY_FIELD = "started_by";
    public static final String STOPPED_BY_FIELD = "stopped_by";
    public static final String ERROR_FIELD = "error";
    public static final String STATE_FIELD = "state";
    public static final String DETECTOR_ID_FIELD = "detector_id";
    public static final String TASK_PROGRESS_FIELD = "task_progress";
    public static final String INIT_PROGRESS_FIELD = "init_progress";
    public static final String CURRENT_PIECE_FIELD = "current_piece";
    public static final String EXECUTION_START_TIME_FIELD = "execution_start_time";
    public static final String EXECUTION_END_TIME_FIELD = "execution_end_time";
    public static final String IS_LATEST_FIELD = "is_latest";
    public static final String TASK_TYPE_FIELD = "task_type";
    public static final String CHECKPOINT_ID_FIELD = "checkpoint_id";
    public static final String COORDINATING_NODE_FIELD = "coordinating_node";
    public static final String WORKER_NODE_FIELD = "worker_node";
    public static final String DETECTOR_FIELD = "detector";
    public static final String USER_FIELD = "user";

    private String taskId = null;
    private Instant lastUpdateTime = null;
    private String startedBy = null;
    private String stoppedBy = null;
    private String error = null;
    private String state = null;
    private String detectorId = null;
    private Float taskProgress = null;
    private Float initProgress = null;
    private Instant currentPiece = null;
    private Instant executionStartTime = null;
    private Instant executionEndTime = null;
    private Boolean isLatest = null;
    private String taskType = null;
    private String checkpointId = null;
    private AnomalyDetector detector = null;

    private String coordinatingNode = null;
    private String workerNode = null;
    private User user = null;

    private ADTask() {}

    public ADTask(StreamInput input) throws IOException {
        this.taskId = input.readOptionalString();
        this.taskType = input.readOptionalString();
        this.detectorId = input.readOptionalString();
        if (input.readBoolean()) {
            this.detector = new AnomalyDetector(input);
        } else {
            this.detector = null;
        }
        this.state = input.readOptionalString();
        this.taskProgress = input.readOptionalFloat();
        this.initProgress = input.readOptionalFloat();
        this.currentPiece = input.readOptionalInstant();
        this.executionStartTime = input.readOptionalInstant();
        this.executionEndTime = input.readOptionalInstant();
        this.isLatest = input.readOptionalBoolean();
        this.error = input.readOptionalString();
        this.checkpointId = input.readOptionalString();
        this.lastUpdateTime = input.readOptionalInstant();
        this.startedBy = input.readOptionalString();
        this.stoppedBy = input.readOptionalString();
        this.coordinatingNode = input.readOptionalString();
        this.workerNode = input.readOptionalString();
        if (input.readBoolean()) {
            this.user = new User(input);
        } else {
            user = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(taskId);
        out.writeOptionalString(taskType);
        out.writeOptionalString(detectorId);
        if (detector != null) {
            out.writeBoolean(true);
            detector.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalString(state);
        out.writeOptionalFloat(taskProgress);
        out.writeOptionalFloat(initProgress);
        out.writeOptionalInstant(currentPiece);
        out.writeOptionalInstant(executionStartTime);
        out.writeOptionalInstant(executionEndTime);
        out.writeOptionalBoolean(isLatest);
        out.writeOptionalString(error);
        out.writeOptionalString(checkpointId);
        out.writeOptionalInstant(lastUpdateTime);
        out.writeOptionalString(startedBy);
        out.writeOptionalString(stoppedBy);
        out.writeOptionalString(coordinatingNode);
        out.writeOptionalString(workerNode);
        if (user != null) {
            out.writeBoolean(true); // user exists
            user.writeTo(out);
        } else {
            out.writeBoolean(false); // user does not exist
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String taskId = null;
        private String taskType = null;
        private String detectorId = null;
        private AnomalyDetector detector = null;
        private String state = null;
        private Float taskProgress = null;
        private Float initProgress = null;
        private Instant currentPiece = null;
        private Instant executionStartTime = null;
        private Instant executionEndTime = null;
        private Boolean isLatest = null;
        private String error = null;
        private String checkpointId = null;
        private Instant lastUpdateTime = null;
        private String startedBy = null;
        private String stoppedBy = null;
        private String coordinatingNode = null;
        private String workerNode = null;
        private User user = null;

        public Builder() {}

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder lastUpdateTime(Instant lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        public Builder startedBy(String startedBy) {
            this.startedBy = startedBy;
            return this;
        }

        public Builder stoppedBy(String stoppedBy) {
            this.stoppedBy = stoppedBy;
            return this;
        }

        public Builder error(String error) {
            this.error = error;
            return this;
        }

        public Builder state(String state) {
            this.state = state;
            return this;
        }

        public Builder detectorId(String detectorId) {
            this.detectorId = detectorId;
            return this;
        }

        public Builder taskProgress(Float taskProgress) {
            this.taskProgress = taskProgress;
            return this;
        }

        public Builder initProgress(Float initProgress) {
            this.initProgress = initProgress;
            return this;
        }

        public Builder currentPiece(Instant currentPiece) {
            this.currentPiece = currentPiece;
            return this;
        }

        public Builder executionStartTime(Instant executionStartTime) {
            this.executionStartTime = executionStartTime;
            return this;
        }

        public Builder executionEndTime(Instant executionEndTime) {
            this.executionEndTime = executionEndTime;
            return this;
        }

        public Builder isLatest(Boolean isLatest) {
            this.isLatest = isLatest;
            return this;
        }

        public Builder taskType(String taskType) {
            this.taskType = taskType;
            return this;
        }

        public Builder checkpointId(String checkpointId) {
            this.checkpointId = checkpointId;
            return this;
        }

        public Builder detector(AnomalyDetector detector) {
            this.detector = detector;
            return this;
        }

        public Builder coordinatingNode(String coordinatingNode) {
            this.coordinatingNode = coordinatingNode;
            return this;
        }

        public Builder workerNode(String workerNode) {
            this.workerNode = workerNode;
            return this;
        }

        public Builder user(User user) {
            this.user = user;
            return this;
        }

        public ADTask build() {
            ADTask adTask = new ADTask();
            adTask.taskId = this.taskId;
            adTask.lastUpdateTime = this.lastUpdateTime;
            adTask.error = this.error;
            adTask.state = this.state;
            adTask.detectorId = this.detectorId;
            adTask.taskProgress = this.taskProgress;
            adTask.initProgress = this.initProgress;
            adTask.currentPiece = this.currentPiece;
            adTask.executionStartTime = this.executionStartTime;
            adTask.executionEndTime = this.executionEndTime;
            adTask.isLatest = this.isLatest;
            adTask.taskType = this.taskType;
            adTask.checkpointId = this.checkpointId;
            adTask.detector = this.detector;
            adTask.startedBy = this.startedBy;
            adTask.stoppedBy = this.stoppedBy;
            adTask.coordinatingNode = this.coordinatingNode;
            adTask.workerNode = this.workerNode;
            adTask.user = this.user;

            return adTask;
        }

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        if (taskId != null) {
            xContentBuilder.field(TASK_ID_FIELD, taskId);
        }
        if (lastUpdateTime != null) {
            xContentBuilder.field(LAST_UPDATE_TIME_FIELD, lastUpdateTime.toEpochMilli());
        }
        if (startedBy != null) {
            xContentBuilder.field(STARTED_BY_FIELD, startedBy);
        }
        if (stoppedBy != null) {
            xContentBuilder.field(STOPPED_BY_FIELD, stoppedBy);
        }
        if (error != null) {
            xContentBuilder.field(ERROR_FIELD, error);
        }
        if (state != null) {
            xContentBuilder.field(STATE_FIELD, state);
        }
        if (detectorId != null) {
            xContentBuilder.field(DETECTOR_ID_FIELD, detectorId);
        }
        if (taskProgress != null) {
            xContentBuilder.field(TASK_PROGRESS_FIELD, taskProgress);
        }
        if (initProgress != null) {
            xContentBuilder.field(INIT_PROGRESS_FIELD, initProgress);
        }
        if (currentPiece != null) {
            xContentBuilder.field(CURRENT_PIECE_FIELD, currentPiece.toEpochMilli());
        }
        if (executionStartTime != null) {
            xContentBuilder.field(EXECUTION_START_TIME_FIELD, executionStartTime.toEpochMilli());
        }
        if (executionEndTime != null) {
            xContentBuilder.field(EXECUTION_END_TIME_FIELD, executionEndTime.toEpochMilli());
        }
        if (isLatest != null) {
            xContentBuilder.field(IS_LATEST_FIELD, isLatest);
        }
        if (taskType != null) {
            xContentBuilder.field(TASK_TYPE_FIELD, taskType);
        }
        if (checkpointId != null) {
            xContentBuilder.field(CHECKPOINT_ID_FIELD, checkpointId);
        }
        if (coordinatingNode != null) {
            xContentBuilder.field(COORDINATING_NODE_FIELD, coordinatingNode);
        }
        if (workerNode != null) {
            xContentBuilder.field(WORKER_NODE_FIELD, workerNode);
        }
        if (detector != null) {
            xContentBuilder.field(DETECTOR_FIELD, detector);
        }
        if (user != null) {
            xContentBuilder.field(USER_FIELD, user);
        }
        return xContentBuilder.endObject();
    }

    public static ADTask parse(XContentParser parser) throws IOException {
        return parse(parser, null);
    }

    public static ADTask parse(XContentParser parser, String taskId) throws IOException {
        Instant lastUpdateTime = null;
        String startedBy = null;
        String stoppedBy = null;
        String error = null;
        String state = null;
        String detectorId = null;
        Float taskProgress = null;
        Float initProgress = null;
        Instant currentPiece = null;
        Instant executionStartTime = null;
        Instant executionEndTime = null;
        Boolean isLatest = null;
        String taskType = null;
        String checkpointId = null;
        AnomalyDetector detector = null;
        String parsedTaskId = taskId;
        String coordinatingNode = null;
        String workerNode = null;
        User user = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case LAST_UPDATE_TIME_FIELD:
                    lastUpdateTime = ParseUtils.toInstant(parser);
                    break;
                case STARTED_BY_FIELD:
                    startedBy = parser.text();
                    break;
                case STOPPED_BY_FIELD:
                    stoppedBy = parser.text();
                    break;
                case ERROR_FIELD:
                    error = parser.text();
                    break;
                case STATE_FIELD:
                    state = parser.text();
                    break;
                case DETECTOR_ID_FIELD:
                    detectorId = parser.text();
                    break;
                case TASK_PROGRESS_FIELD:
                    taskProgress = parser.floatValue();
                    break;
                case INIT_PROGRESS_FIELD:
                    initProgress = parser.floatValue();
                    break;
                case CURRENT_PIECE_FIELD:
                    currentPiece = ParseUtils.toInstant(parser);
                    break;
                case EXECUTION_START_TIME_FIELD:
                    executionStartTime = ParseUtils.toInstant(parser);
                    break;
                case EXECUTION_END_TIME_FIELD:
                    executionEndTime = ParseUtils.toInstant(parser);
                    break;
                case IS_LATEST_FIELD:
                    isLatest = parser.booleanValue();
                    break;
                case TASK_TYPE_FIELD:
                    taskType = parser.text();
                    break;
                case CHECKPOINT_ID_FIELD:
                    checkpointId = parser.text();
                    break;
                case DETECTOR_FIELD:
                    detector = AnomalyDetector.parse(parser);
                    break;
                case TASK_ID_FIELD:
                    parsedTaskId = parser.text();
                    break;
                case COORDINATING_NODE_FIELD:
                    coordinatingNode = parser.text();
                    break;
                case WORKER_NODE_FIELD:
                    workerNode = parser.text();
                    break;
                case USER_FIELD:
                    user = User.parse(parser);
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        AnomalyDetector anomalyDetector = detector == null
            ? null
            : new AnomalyDetector(
                detectorId,
                detector.getVersion(),
                detector.getName(),
                detector.getDescription(),
                detector.getTimeField(),
                detector.getIndices(),
                detector.getFeatureAttributes(),
                detector.getFilterQuery(),
                detector.getDetectionInterval(),
                detector.getWindowDelay(),
                detector.getShingleSize(),
                detector.getUiMetadata(),
                detector.getSchemaVersion(),
                detector.getLastUpdateTime(),
                detector.getCategoryField(),
                detector.getUser(),
                detector.getDetectorType(),
                detector.getDetectionDateRange()
            );
        return new Builder()
            .taskId(parsedTaskId)
            .lastUpdateTime(lastUpdateTime)
            .startedBy(startedBy)
            .stoppedBy(stoppedBy)
            .error(error)
            .state(state)
            .detectorId(detectorId)
            .taskProgress(taskProgress)
            .initProgress(initProgress)
            .currentPiece(currentPiece)
            .executionStartTime(executionStartTime)
            .executionEndTime(executionEndTime)
            .isLatest(isLatest)
            .taskType(taskType)
            .checkpointId(checkpointId)
            .coordinatingNode(coordinatingNode)
            .workerNode(workerNode)
            .detector(anomalyDetector)
            .user(user)
            .build();
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ADTask that = (ADTask) o;
        return Objects.equal(getTaskId(), that.getTaskId())
            && Objects.equal(getLastUpdateTime(), that.getLastUpdateTime())
            && Objects.equal(getStartedBy(), that.getStartedBy())
            && Objects.equal(getStoppedBy(), that.getStoppedBy())
            && Objects.equal(getError(), that.getError())
            && Objects.equal(getState(), that.getState())
            && Objects.equal(getDetectorId(), that.getDetectorId())
            && Objects.equal(getTaskProgress(), that.getTaskProgress())
            && Objects.equal(getInitProgress(), that.getInitProgress())
            && Objects.equal(getCurrentPiece(), that.getCurrentPiece())
            && Objects.equal(getExecutionStartTime(), that.getExecutionStartTime())
            && Objects.equal(getExecutionEndTime(), that.getExecutionEndTime())
            && Objects.equal(getLatest(), that.getLatest())
            && Objects.equal(getTaskType(), that.getTaskType())
            && Objects.equal(getCheckpointId(), that.getCheckpointId())
            && Objects.equal(getCoordinatingNode(), that.getCoordinatingNode())
            && Objects.equal(getWorkerNode(), that.getWorkerNode())
            && Objects.equal(getDetector(), that.getDetector())
            && Objects.equal(getUser(), that.getUser());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects
            .hashCode(
                taskId,
                lastUpdateTime,
                startedBy,
                stoppedBy,
                error,
                state,
                detectorId,
                taskProgress,
                initProgress,
                currentPiece,
                executionStartTime,
                executionEndTime,
                isLatest,
                taskType,
                checkpointId,
                coordinatingNode,
                workerNode,
                detector,
                user
            );
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    public String getStartedBy() {
        return startedBy;
    }

    public String getStoppedBy() {
        return stoppedBy;
    }

    public String getError() {
        return error;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getDetectorId() {
        return detectorId;
    }

    public Float getTaskProgress() {
        return taskProgress;
    }

    public Float getInitProgress() {
        return initProgress;
    }

    public Instant getCurrentPiece() {
        return currentPiece;
    }

    public Instant getExecutionStartTime() {
        return executionStartTime;
    }

    public Instant getExecutionEndTime() {
        return executionEndTime;
    }

    public Boolean getLatest() {
        return isLatest;
    }

    public String getTaskType() {
        return taskType;
    }

    public String getCheckpointId() {
        return checkpointId;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public String getCoordinatingNode() {
        return coordinatingNode;
    }

    public String getWorkerNode() {
        return workerNode;
    }

    public User getUser() {
        return user;
    }
}
