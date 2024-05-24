/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.model;

import static org.opensearch.timeseries.model.TaskState.NOT_ENDED_STATES;

import java.io.IOException;
import java.time.Instant;

import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.annotation.Generated;

import com.google.common.base.Objects;

public abstract class TimeSeriesTask implements ToXContentObject, Writeable {

    public static final String TASK_ID_FIELD = "task_id";
    public static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    public static final String STARTED_BY_FIELD = "started_by";
    public static final String STOPPED_BY_FIELD = "stopped_by";
    public static final String ERROR_FIELD = "error";
    public static final String STATE_FIELD = "state";
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
    public static final String ENTITY_FIELD = "entity";
    public static final String PARENT_TASK_ID_FIELD = "parent_task_id";
    public static final String ESTIMATED_MINUTES_LEFT_FIELD = "estimated_minutes_left";
    public static final String USER_FIELD = "user";
    public static final String HISTORICAL_TASK_PREFIX = "HISTORICAL";
    public static final String RUN_ONCE_TASK_PREFIX = "RUN_ONCE";
    public static final String REAL_TIME_TASK_PREFIX = "REALTIME";

    protected String configId = null;
    protected String taskId = null;
    protected Instant lastUpdateTime = null;
    protected String startedBy = null;
    protected String stoppedBy = null;
    protected String error = null;
    protected String state = null;
    protected Float taskProgress = null;
    protected Float initProgress = null;
    protected Instant currentPiece = null;
    protected Instant executionStartTime = null;
    protected Instant executionEndTime = null;
    protected Boolean isLatest = null;
    protected String taskType = null;
    protected String checkpointId = null;
    protected String coordinatingNode = null;
    protected String workerNode = null;
    protected Entity entity = null;
    protected String parentTaskId = null;
    protected Integer estimatedMinutesLeft = null;
    protected User user = null;

    @SuppressWarnings("unchecked")
    public abstract static class Builder<T extends Builder<T>> {
        protected String configId = null;
        protected String taskId = null;
        protected String taskType = null;
        protected String state = null;
        protected Float taskProgress = null;
        protected Float initProgress = null;
        protected Instant currentPiece = null;
        protected Instant executionStartTime = null;
        protected Instant executionEndTime = null;
        protected Boolean isLatest = null;
        protected String error = null;
        protected String checkpointId = null;
        protected Instant lastUpdateTime = null;
        protected String startedBy = null;
        protected String stoppedBy = null;
        protected String coordinatingNode = null;
        protected String workerNode = null;
        protected Entity entity = null;
        protected String parentTaskId;
        protected Integer estimatedMinutesLeft;
        protected User user = null;

        public Builder() {}

        public T configId(String configId) {
            this.configId = configId;
            return (T) this;
        }

        public T taskId(String taskId) {
            this.taskId = taskId;
            return (T) this;
        }

        public T lastUpdateTime(Instant lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return (T) this;
        }

        public T startedBy(String startedBy) {
            this.startedBy = startedBy;
            return (T) this;
        }

        public T stoppedBy(String stoppedBy) {
            this.stoppedBy = stoppedBy;
            return (T) this;
        }

        public T error(String error) {
            this.error = error;
            return (T) this;
        }

        public T state(String state) {
            this.state = state;
            return (T) this;
        }

        public T taskProgress(Float taskProgress) {
            this.taskProgress = taskProgress;
            return (T) this;
        }

        public T initProgress(Float initProgress) {
            this.initProgress = initProgress;
            return (T) this;
        }

        public T currentPiece(Instant currentPiece) {
            this.currentPiece = currentPiece;
            return (T) this;
        }

        public T executionStartTime(Instant executionStartTime) {
            this.executionStartTime = executionStartTime;
            return (T) this;
        }

        public T executionEndTime(Instant executionEndTime) {
            this.executionEndTime = executionEndTime;
            return (T) this;
        }

        public T isLatest(Boolean isLatest) {
            this.isLatest = isLatest;
            return (T) this;
        }

        public T taskType(String taskType) {
            this.taskType = taskType;
            return (T) this;
        }

        public T checkpointId(String checkpointId) {
            this.checkpointId = checkpointId;
            return (T) this;
        }

        public T coordinatingNode(String coordinatingNode) {
            this.coordinatingNode = coordinatingNode;
            return (T) this;
        }

        public T workerNode(String workerNode) {
            this.workerNode = workerNode;
            return (T) this;
        }

        public T entity(Entity entity) {
            this.entity = entity;
            return (T) this;
        }

        public T parentTaskId(String parentTaskId) {
            this.parentTaskId = parentTaskId;
            return (T) this;
        }

        public T estimatedMinutesLeft(Integer estimatedMinutesLeft) {
            this.estimatedMinutesLeft = estimatedMinutesLeft;
            return (T) this;
        }

        public T user(User user) {
            this.user = user;
            return (T) this;
        }
    }

    public boolean isHistoricalTask() {
        return taskType.startsWith(TimeSeriesTask.HISTORICAL_TASK_PREFIX);
    }

    public boolean isRunOnceTask() {
        return taskType.startsWith(TimeSeriesTask.RUN_ONCE_TASK_PREFIX);
    }

    public boolean isRealTimeTask() {
        return taskType.startsWith(TimeSeriesTask.REAL_TIME_TASK_PREFIX);
    }

    /**
     * Get config level task id. If a task has no parent task, the task is config level task.
     * @return config level task id
     */
    public String getConfigLevelTaskId() {
        return getParentTaskId() != null ? getParentTaskId() : getTaskId();
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

    public void setError(String error) {
        this.error = error;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
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

    public Boolean isLatest() {
        return isLatest;
    }

    public String getTaskType() {
        return taskType;
    }

    public String getCheckpointId() {
        return checkpointId;
    }

    public String getCoordinatingNode() {
        return coordinatingNode;
    }

    public String getWorkerNode() {
        return workerNode;
    }

    public Entity getEntity() {
        return entity;
    }

    public String getParentTaskId() {
        return parentTaskId;
    }

    public Integer getEstimatedMinutesLeft() {
        return estimatedMinutesLeft;
    }

    public User getUser() {
        return user;
    }

    public String getConfigId() {
        return configId;
    }

    public void setLatest(Boolean latest) {
        isLatest = latest;
    }

    public void setLastUpdateTime(Instant lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public boolean isDone() {
        return !NOT_ENDED_STATES.contains(this.getState());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (taskId != null) {
            builder.field(TimeSeriesTask.TASK_ID_FIELD, taskId);
        }
        if (lastUpdateTime != null) {
            builder.field(TimeSeriesTask.LAST_UPDATE_TIME_FIELD, lastUpdateTime.toEpochMilli());
        }
        if (startedBy != null) {
            builder.field(TimeSeriesTask.STARTED_BY_FIELD, startedBy);
        }
        if (stoppedBy != null) {
            builder.field(TimeSeriesTask.STOPPED_BY_FIELD, stoppedBy);
        }
        if (error != null) {
            builder.field(TimeSeriesTask.ERROR_FIELD, error);
        }
        if (state != null) {
            builder.field(TimeSeriesTask.STATE_FIELD, state);
        }
        if (taskProgress != null) {
            builder.field(TimeSeriesTask.TASK_PROGRESS_FIELD, taskProgress);
        }
        if (initProgress != null) {
            builder.field(TimeSeriesTask.INIT_PROGRESS_FIELD, initProgress);
        }
        if (currentPiece != null) {
            builder.field(TimeSeriesTask.CURRENT_PIECE_FIELD, currentPiece.toEpochMilli());
        }
        if (executionStartTime != null) {
            builder.field(TimeSeriesTask.EXECUTION_START_TIME_FIELD, executionStartTime.toEpochMilli());
        }
        if (executionEndTime != null) {
            builder.field(TimeSeriesTask.EXECUTION_END_TIME_FIELD, executionEndTime.toEpochMilli());
        }
        if (isLatest != null) {
            builder.field(TimeSeriesTask.IS_LATEST_FIELD, isLatest);
        }
        if (taskType != null) {
            builder.field(TimeSeriesTask.TASK_TYPE_FIELD, taskType);
        }
        if (checkpointId != null) {
            builder.field(TimeSeriesTask.CHECKPOINT_ID_FIELD, checkpointId);
        }
        if (coordinatingNode != null) {
            builder.field(TimeSeriesTask.COORDINATING_NODE_FIELD, coordinatingNode);
        }
        if (workerNode != null) {
            builder.field(TimeSeriesTask.WORKER_NODE_FIELD, workerNode);
        }
        if (entity != null) {
            builder.field(TimeSeriesTask.ENTITY_FIELD, entity);
        }
        if (parentTaskId != null) {
            builder.field(TimeSeriesTask.PARENT_TASK_ID_FIELD, parentTaskId);
        }
        if (estimatedMinutesLeft != null) {
            builder.field(TimeSeriesTask.ESTIMATED_MINUTES_LEFT_FIELD, estimatedMinutesLeft);
        }
        if (user != null) {
            builder.field(TimeSeriesTask.USER_FIELD, user);
        }
        return builder;
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TimeSeriesTask that = (TimeSeriesTask) o;
        return Objects.equal(getConfigId(), that.getConfigId())
            && Objects.equal(getTaskId(), that.getTaskId())
            && Objects.equal(getLastUpdateTime(), that.getLastUpdateTime())
            && Objects.equal(getStartedBy(), that.getStartedBy())
            && Objects.equal(getStoppedBy(), that.getStoppedBy())
            && Objects.equal(getError(), that.getError())
            && Objects.equal(getState(), that.getState())
            && Objects.equal(getTaskProgress(), that.getTaskProgress())
            && Objects.equal(getInitProgress(), that.getInitProgress())
            && Objects.equal(getCurrentPiece(), that.getCurrentPiece())
            && Objects.equal(getExecutionStartTime(), that.getExecutionStartTime())
            && Objects.equal(getExecutionEndTime(), that.getExecutionEndTime())
            && Objects.equal(isLatest(), that.isLatest())
            && Objects.equal(getTaskType(), that.getTaskType())
            && Objects.equal(getCheckpointId(), that.getCheckpointId())
            && Objects.equal(getCoordinatingNode(), that.getCoordinatingNode())
            && Objects.equal(getWorkerNode(), that.getWorkerNode())
            && Objects.equal(getEntity(), that.getEntity())
            && Objects.equal(getParentTaskId(), that.getParentTaskId())
            && Objects.equal(getEstimatedMinutesLeft(), that.getEstimatedMinutesLeft())
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
                entity,
                parentTaskId,
                estimatedMinutesLeft,
                user
            );
    }

    public abstract boolean isHistoricalEntityTask();

    public String getEntityModelId() {
        return entity == null ? null : entity.getModelId(configId).orElse(null);
    }
}
