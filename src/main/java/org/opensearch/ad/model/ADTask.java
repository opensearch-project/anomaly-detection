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
import java.time.Instant;

import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.timeseries.annotation.Generated;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.util.ParseUtils;

import com.google.common.base.Objects;

/**
 * One anomaly detection task means one detector starts to run until stopped.
 */
public class ADTask extends TimeSeriesTask {

    public static final String DETECTOR_ID_FIELD = "detector_id";
    public static final String DETECTOR_FIELD = "detector";
    public static final String DETECTION_DATE_RANGE_FIELD = "detection_date_range";

    private AnomalyDetector detector = null;
    private DateRange detectionDateRange = null;

    private ADTask() {}

    public ADTask(StreamInput input) throws IOException {
        this.taskId = input.readOptionalString();
        this.taskType = input.readOptionalString();
        this.configId = input.readOptionalString();
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
        // Below are new fields added since AD 1.1
        if (input.available() > 0) {
            if (input.readBoolean()) {
                this.detectionDateRange = new DateRange(input);
            } else {
                this.detectionDateRange = null;
            }
            if (input.readBoolean()) {
                this.entity = new Entity(input);
            } else {
                this.entity = null;
            }
            this.parentTaskId = input.readOptionalString();
            this.estimatedMinutesLeft = input.readOptionalInt();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(taskId);
        out.writeOptionalString(taskType);
        out.writeOptionalString(configId);
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
        // Only forward AD task to nodes with same version, so it's ok to write these new fields.
        if (detectionDateRange != null) {
            out.writeBoolean(true);
            detectionDateRange.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        if (entity != null) {
            out.writeBoolean(true);
            entity.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalString(parentTaskId);
        out.writeOptionalInt(estimatedMinutesLeft);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean isHistoricalEntityTask() {
        return ADTaskType.HISTORICAL_HC_ENTITY.name().equals(taskType);
    }

    public static class Builder extends TimeSeriesTask.Builder<Builder> {
        private AnomalyDetector detector = null;
        private DateRange detectionDateRange = null;

        public Builder() {}

        public Builder detector(AnomalyDetector detector) {
            this.detector = detector;
            return this;
        }

        public Builder detectionDateRange(DateRange detectionDateRange) {
            this.detectionDateRange = detectionDateRange;
            return this;
        }

        public ADTask build() {
            ADTask adTask = new ADTask();
            adTask.taskId = this.taskId;
            adTask.lastUpdateTime = this.lastUpdateTime;
            adTask.error = this.error;
            adTask.state = this.state;
            adTask.configId = this.configId;
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
            adTask.detectionDateRange = this.detectionDateRange;
            adTask.entity = this.entity;
            adTask.parentTaskId = this.parentTaskId;
            adTask.estimatedMinutesLeft = this.estimatedMinutesLeft;
            adTask.user = this.user;

            return adTask;
        }

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder = super.toXContent(xContentBuilder, params);
        if (configId != null) {
            xContentBuilder.field(DETECTOR_ID_FIELD, configId);
        }
        if (detector != null) {
            xContentBuilder.field(DETECTOR_FIELD, detector);
        }
        if (detectionDateRange != null) {
            xContentBuilder.field(DETECTION_DATE_RANGE_FIELD, detectionDateRange);
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
        DateRange detectionDateRange = null;
        Entity entity = null;
        String parentTaskId = null;
        Integer estimatedMinutesLeft = null;
        User user = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case TimeSeriesTask.LAST_UPDATE_TIME_FIELD:
                    lastUpdateTime = ParseUtils.toInstant(parser);
                    break;
                case TimeSeriesTask.STARTED_BY_FIELD:
                    startedBy = parser.text();
                    break;
                case TimeSeriesTask.STOPPED_BY_FIELD:
                    stoppedBy = parser.text();
                    break;
                case TimeSeriesTask.ERROR_FIELD:
                    error = parser.text();
                    break;
                case TimeSeriesTask.STATE_FIELD:
                    state = parser.text();
                    break;
                case DETECTOR_ID_FIELD:
                    detectorId = parser.text();
                    break;
                case TimeSeriesTask.TASK_PROGRESS_FIELD:
                    taskProgress = parser.floatValue();
                    break;
                case TimeSeriesTask.INIT_PROGRESS_FIELD:
                    initProgress = parser.floatValue();
                    break;
                case TimeSeriesTask.CURRENT_PIECE_FIELD:
                    currentPiece = ParseUtils.toInstant(parser);
                    break;
                case TimeSeriesTask.EXECUTION_START_TIME_FIELD:
                    executionStartTime = ParseUtils.toInstant(parser);
                    break;
                case TimeSeriesTask.EXECUTION_END_TIME_FIELD:
                    executionEndTime = ParseUtils.toInstant(parser);
                    break;
                case TimeSeriesTask.IS_LATEST_FIELD:
                    isLatest = parser.booleanValue();
                    break;
                case TimeSeriesTask.TASK_TYPE_FIELD:
                    taskType = parser.text();
                    break;
                case TimeSeriesTask.CHECKPOINT_ID_FIELD:
                    checkpointId = parser.text();
                    break;
                case DETECTOR_FIELD:
                    detector = AnomalyDetector.parse(parser);
                    break;
                case TimeSeriesTask.TASK_ID_FIELD:
                    parsedTaskId = parser.text();
                    break;
                case TimeSeriesTask.COORDINATING_NODE_FIELD:
                    coordinatingNode = parser.text();
                    break;
                case TimeSeriesTask.WORKER_NODE_FIELD:
                    workerNode = parser.text();
                    break;
                case DETECTION_DATE_RANGE_FIELD:
                    detectionDateRange = DateRange.parse(parser);
                    break;
                case TimeSeriesTask.ENTITY_FIELD:
                    entity = Entity.parse(parser);
                    break;
                case TimeSeriesTask.PARENT_TASK_ID_FIELD:
                    parentTaskId = parser.text();
                    break;
                case TimeSeriesTask.ESTIMATED_MINUTES_LEFT_FIELD:
                    estimatedMinutesLeft = parser.intValue();
                    break;
                case TimeSeriesTask.USER_FIELD:
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
                detector.getInterval(),
                detector.getWindowDelay(),
                detector.getShingleSize(),
                detector.getUiMetadata(),
                detector.getSchemaVersion(),
                detector.getLastUpdateTime(),
                detector.getCategoryFields(),
                detector.getUser(),
                detector.getCustomResultIndexOrAlias(),
                detector.getImputationOption(),
                detector.getRecencyEmphasis(),
                detector.getSeasonIntervals(),
                detector.getHistoryIntervals(),
                detector.getRules(),
                detector.getCustomResultIndexMinSize(),
                detector.getCustomResultIndexMinAge(),
                detector.getCustomResultIndexTTL(),
                detector.getFlattenResultIndexMapping(),
                detector.getLastBreakingUIChangeTime()
            );
        return new Builder()
            .taskId(parsedTaskId)
            .lastUpdateTime(lastUpdateTime)
            .startedBy(startedBy)
            .stoppedBy(stoppedBy)
            .error(error)
            .state(state)
            .configId(detectorId)
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
            .detectionDateRange(detectionDateRange)
            .entity(entity)
            .parentTaskId(parentTaskId)
            .estimatedMinutesLeft(estimatedMinutesLeft)
            .user(user)
            .build();
    }

    @Generated
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        ADTask that = (ADTask) other;
        return super.equals(that)
            && Objects.equal(getDetector(), that.getDetector())
            && Objects.equal(getDetectionDateRange(), that.getDetectionDateRange());
    }

    @Generated
    @Override
    public int hashCode() {
        int superHashCode = super.hashCode();
        int hash = Objects.hashCode(configId, detector, detectionDateRange);
        hash += 89 * superHashCode;
        return hash;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public DateRange getDetectionDateRange() {
        return detectionDateRange;
    }

    public void setDetectionDateRange(DateRange detectionDateRange) {
        this.detectionDateRange = detectionDateRange;
    }
}
