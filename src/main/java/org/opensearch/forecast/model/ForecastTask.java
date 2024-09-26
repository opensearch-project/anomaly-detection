/*
<<<<<<< HEAD
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
=======
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
>>>>>>> f22eaa95 (test)
 */

package org.opensearch.forecast.model;

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

public class ForecastTask extends TimeSeriesTask {
    public static final String FORECASTER_ID_FIELD = "forecaster_id";
    public static final String FORECASTER_FIELD = "forecaster";
    public static final String DATE_RANGE_FIELD = "date_range";

    private Forecaster forecaster = null;
    private DateRange dateRange = null;

    private ForecastTask() {}

    public ForecastTask(StreamInput input) throws IOException {
        this.taskId = input.readOptionalString();
        this.taskType = input.readOptionalString();
        this.configId = input.readOptionalString();
        if (input.readBoolean()) {
            this.forecaster = new Forecaster(input);
        } else {
            this.forecaster = null;
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
        if (input.readBoolean()) {
            this.dateRange = new DateRange(input);
        } else {
            this.dateRange = null;
        }
        if (input.readBoolean()) {
            this.entity = new Entity(input);
        } else {
            this.entity = null;
        }
        this.parentTaskId = input.readOptionalString();
        this.estimatedMinutesLeft = input.readOptionalInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(taskId);
        out.writeOptionalString(taskType);
        out.writeOptionalString(configId);
        if (forecaster != null) {
            out.writeBoolean(true);
            forecaster.writeTo(out);
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
        // Only forward forecast task to nodes with same version, so it's ok to write these new fields.
        if (dateRange != null) {
            out.writeBoolean(true);
            dateRange.writeTo(out);
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
        // we have no backtesting
        return false;
    }

    public static class Builder extends TimeSeriesTask.Builder<Builder> {
        private Forecaster forecaster = null;
        private DateRange dateRange = null;

        public Builder() {}

        public Builder forecaster(Forecaster forecaster) {
            this.forecaster = forecaster;
            return this;
        }

        public Builder dateRange(DateRange dateRange) {
            this.dateRange = dateRange;
            return this;
        }

        public ForecastTask build() {
            ForecastTask forecastTask = new ForecastTask();
            forecastTask.taskId = this.taskId;
            forecastTask.lastUpdateTime = this.lastUpdateTime;
            forecastTask.error = this.error;
            forecastTask.state = this.state;
            forecastTask.configId = this.configId;
            forecastTask.taskProgress = this.taskProgress;
            forecastTask.initProgress = this.initProgress;
            forecastTask.currentPiece = this.currentPiece;
            forecastTask.executionStartTime = this.executionStartTime;
            forecastTask.executionEndTime = this.executionEndTime;
            forecastTask.isLatest = this.isLatest;
            forecastTask.taskType = this.taskType;
            forecastTask.checkpointId = this.checkpointId;
            forecastTask.forecaster = this.forecaster;
            forecastTask.startedBy = this.startedBy;
            forecastTask.stoppedBy = this.stoppedBy;
            forecastTask.coordinatingNode = this.coordinatingNode;
            forecastTask.workerNode = this.workerNode;
            forecastTask.dateRange = this.dateRange;
            forecastTask.entity = this.entity;
            forecastTask.parentTaskId = this.parentTaskId;
            forecastTask.estimatedMinutesLeft = this.estimatedMinutesLeft;
            forecastTask.user = this.user;

            return forecastTask;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder = super.toXContent(xContentBuilder, params);
        if (configId != null) {
            xContentBuilder.field(FORECASTER_ID_FIELD, configId);
        }
        if (forecaster != null) {
            xContentBuilder.field(FORECASTER_FIELD, forecaster);
        }
        if (dateRange != null) {
            xContentBuilder.field(DATE_RANGE_FIELD, dateRange);
        }
        return xContentBuilder.endObject();
    }

    public static ForecastTask parse(XContentParser parser) throws IOException {
        return parse(parser, null);
    }

    public static ForecastTask parse(XContentParser parser, String taskId) throws IOException {
        Instant lastUpdateTime = null;
        String startedBy = null;
        String stoppedBy = null;
        String error = null;
        String state = null;
        String configId = null;
        Float taskProgress = null;
        Float initProgress = null;
        Instant currentPiece = null;
        Instant executionStartTime = null;
        Instant executionEndTime = null;
        Boolean isLatest = null;
        String taskType = null;
        String checkpointId = null;
        Forecaster forecaster = null;
        String parsedTaskId = taskId;
        String coordinatingNode = null;
        String workerNode = null;
        DateRange dateRange = null;
        Entity entity = null;
        String parentTaskId = null;
        Integer estimatedMinutesLeft = null;
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
                case FORECASTER_ID_FIELD:
                    configId = parser.text();
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
                case FORECASTER_FIELD:
                    forecaster = Forecaster.parse(parser);
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
                case DATE_RANGE_FIELD:
                    dateRange = DateRange.parse(parser);
                    break;
                case ENTITY_FIELD:
                    entity = Entity.parse(parser);
                    break;
                case PARENT_TASK_ID_FIELD:
                    parentTaskId = parser.text();
                    break;
                case ESTIMATED_MINUTES_LEFT_FIELD:
                    estimatedMinutesLeft = parser.intValue();
                    break;
                case USER_FIELD:
                    user = User.parse(parser);
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        Forecaster copyForecaster = forecaster == null
            ? null
            : new Forecaster(
                configId,
                forecaster.getVersion(),
                forecaster.getName(),
                forecaster.getDescription(),
                forecaster.getTimeField(),
                forecaster.getIndices(),
                forecaster.getFeatureAttributes(),
                forecaster.getFilterQuery(),
                forecaster.getInterval(),
                forecaster.getWindowDelay(),
                forecaster.getShingleSize(),
                forecaster.getUiMetadata(),
                forecaster.getSchemaVersion(),
                forecaster.getLastUpdateTime(),
                forecaster.getCategoryFields(),
                forecaster.getUser(),
                forecaster.getCustomResultIndexOrAlias(),
                forecaster.getHorizon(),
                forecaster.getImputationOption(),
                forecaster.getRecencyEmphasis(),
                forecaster.getSeasonIntervals(),
                forecaster.getHistoryIntervals(),
                forecaster.getCustomResultIndexMinSize(),
                forecaster.getCustomResultIndexMinAge(),
                forecaster.getCustomResultIndexTTL(),
                forecaster.getFlattenResultIndexMapping(),
                forecaster.getLastBreakingUIChangeTime()
            );
        return new Builder()
            .taskId(parsedTaskId)
            .lastUpdateTime(lastUpdateTime)
            .startedBy(startedBy)
            .stoppedBy(stoppedBy)
            .error(error)
            .state(state)
            .configId(configId)
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
            .forecaster(copyForecaster)
            .dateRange(dateRange)
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
        ForecastTask that = (ForecastTask) other;
        return super.equals(that)
            && Objects.equal(getForecaster(), that.getForecaster())
            && Objects.equal(getDateRange(), that.getDateRange());
    }

    @Generated
    @Override
    public int hashCode() {
        int superHashCode = super.hashCode();
        int hash = Objects.hashCode(configId, forecaster, dateRange);
        hash += 89 * superHashCode;
        return hash;
    }

    public Forecaster getForecaster() {
        return forecaster;
    }

    public DateRange getDateRange() {
        return dateRange;
    }

    public void setDateRange(DateRange dateRange) {
        this.dateRange = dateRange;
    }
}
