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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.ad.annotation.Generated;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.ad.util.ParseUtils;
import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.commons.authuser.User;

import com.google.common.base.Objects;

/**
 * Include result returned from RCF model and feature data.
 * TODO: fix rotating anomaly result index
 */
public class AnomalyResult implements ToXContentObject, Writeable {

    public static final String PARSE_FIELD_NAME = "AnomalyResult";
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
        AnomalyResult.class,
        new ParseField(PARSE_FIELD_NAME),
        it -> parse(it)
    );

    public static final String DETECTOR_ID_FIELD = "detector_id";
    public static final String ANOMALY_SCORE_FIELD = "anomaly_score";
    private static final String ANOMALY_GRADE_FIELD = "anomaly_grade";
    private static final String CONFIDENCE_FIELD = "confidence";
    private static final String FEATURE_DATA_FIELD = "feature_data";
    private static final String DATA_START_TIME_FIELD = "data_start_time";
    public static final String DATA_END_TIME_FIELD = "data_end_time";
    private static final String EXECUTION_START_TIME_FIELD = "execution_start_time";
    public static final String EXECUTION_END_TIME_FIELD = "execution_end_time";
    public static final String ERROR_FIELD = "error";
    public static final String ENTITY_FIELD = "entity";
    public static final String USER_FIELD = "user";
    public static final String TASK_ID_FIELD = "task_id";

    private final String detectorId;
    private final String taskId;
    private final Double anomalyScore;
    private final Double anomalyGrade;
    private final Double confidence;
    private final List<FeatureData> featureData;
    private final Instant dataStartTime;
    private final Instant dataEndTime;
    private final Instant executionStartTime;
    private final Instant executionEndTime;
    private final String error;
    private final List<Entity> entity;
    private User user;
    private final Integer schemaVersion;

    public AnomalyResult(
        String detectorId,
        Double anomalyScore,
        Double anomalyGrade,
        Double confidence,
        List<FeatureData> featureData,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executionStartTime,
        Instant executionEndTime,
        String error,
        User user,
        Integer schemaVersion
    ) {
        this(
            detectorId,
            anomalyScore,
            anomalyGrade,
            confidence,
            featureData,
            dataStartTime,
            dataEndTime,
            executionStartTime,
            executionEndTime,
            error,
            null,
            user,
            schemaVersion
        );
    }

    public AnomalyResult(
        String detectorId,
        Double anomalyScore,
        Double anomalyGrade,
        Double confidence,
        List<FeatureData> featureData,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executionStartTime,
        Instant executionEndTime,
        String error,
        List<Entity> entity,
        User user,
        Integer schemaVersion
    ) {
        this(
            detectorId,
            null,
            anomalyScore,
            anomalyGrade,
            confidence,
            featureData,
            dataStartTime,
            dataEndTime,
            executionStartTime,
            executionEndTime,
            error,
            entity,
            user,
            schemaVersion
        );
    }

    public AnomalyResult(
        String detectorId,
        String taskId,
        Double anomalyScore,
        Double anomalyGrade,
        Double confidence,
        List<FeatureData> featureData,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executionStartTime,
        Instant executionEndTime,
        String error,
        List<Entity> entity,
        User user,
        Integer schemaVersion
    ) {
        this.detectorId = detectorId;
        this.taskId = taskId;
        this.anomalyScore = anomalyScore;
        this.anomalyGrade = anomalyGrade;
        this.confidence = confidence;
        this.featureData = featureData;
        this.dataStartTime = dataStartTime;
        this.dataEndTime = dataEndTime;
        this.executionStartTime = executionStartTime;
        this.executionEndTime = executionEndTime;
        this.error = error;
        this.entity = entity;
        this.user = user;
        this.schemaVersion = schemaVersion;
    }

    public AnomalyResult(StreamInput input) throws IOException {
        this.detectorId = input.readString();
        this.anomalyScore = input.readDouble();
        this.anomalyGrade = input.readDouble();
        this.confidence = input.readDouble();
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
            int entitySize = input.readVInt();
            this.entity = new ArrayList<>(entitySize);
            for (int i = 0; i < entitySize; i++) {
                entity.add(new Entity(input));
            }
        } else {
            this.entity = null;
        }
        if (input.readBoolean()) {
            this.user = new User(input);
        } else {
            user = null;
        }
        this.schemaVersion = input.readInt();
        this.taskId = input.readOptionalString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(DETECTOR_ID_FIELD, detectorId)
            .field(DATA_START_TIME_FIELD, dataStartTime.toEpochMilli())
            .field(DATA_END_TIME_FIELD, dataEndTime.toEpochMilli())
            .field(CommonName.SCHEMA_VERSION_FIELD, schemaVersion);
        if (featureData != null) {
            // can be null during preview
            xContentBuilder.field(FEATURE_DATA_FIELD, featureData.toArray());
        }
        if (executionStartTime != null) {
            // can be null during preview
            xContentBuilder.field(EXECUTION_START_TIME_FIELD, executionStartTime.toEpochMilli());
        }
        if (executionEndTime != null) {
            // can be null during preview
            xContentBuilder.field(EXECUTION_END_TIME_FIELD, executionEndTime.toEpochMilli());
        }
        if (anomalyScore != null && !anomalyScore.isNaN()) {
            xContentBuilder.field(ANOMALY_SCORE_FIELD, anomalyScore);
        }
        if (anomalyGrade != null && !anomalyGrade.isNaN()) {
            xContentBuilder.field(ANOMALY_GRADE_FIELD, anomalyGrade);
        }
        if (confidence != null && !confidence.isNaN()) {
            xContentBuilder.field(CONFIDENCE_FIELD, confidence);
        }
        if (error != null) {
            xContentBuilder.field(ERROR_FIELD, error);
        }
        if (entity != null) {
            xContentBuilder.field(ENTITY_FIELD, entity.toArray());
        }
        if (user != null) {
            xContentBuilder.field(USER_FIELD, user);
        }
        if (taskId != null) {
            xContentBuilder.field(TASK_ID_FIELD, taskId);
        }
        return xContentBuilder.endObject();
    }

    public static AnomalyResult parse(XContentParser parser) throws IOException {
        String detectorId = null;
        Double anomalyScore = null;
        Double anomalyGrade = null;
        Double confidence = null;
        List<FeatureData> featureData = new ArrayList<>();
        Instant dataStartTime = null;
        Instant dataEndTime = null;
        Instant executionStartTime = null;
        Instant executionEndTime = null;
        String error = null;
        List<Entity> entityList = null;
        User user = null;
        Integer schemaVersion = CommonValue.NO_SCHEMA_VERSION;
        String taskId = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case DETECTOR_ID_FIELD:
                    detectorId = parser.text();
                    break;
                case ANOMALY_SCORE_FIELD:
                    anomalyScore = parser.doubleValue();
                    break;
                case ANOMALY_GRADE_FIELD:
                    anomalyGrade = parser.doubleValue();
                    break;
                case CONFIDENCE_FIELD:
                    confidence = parser.doubleValue();
                    break;
                case FEATURE_DATA_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        featureData.add(FeatureData.parse(parser));
                    }
                    break;
                case DATA_START_TIME_FIELD:
                    dataStartTime = ParseUtils.toInstant(parser);
                    break;
                case DATA_END_TIME_FIELD:
                    dataEndTime = ParseUtils.toInstant(parser);
                    break;
                case EXECUTION_START_TIME_FIELD:
                    executionStartTime = ParseUtils.toInstant(parser);
                    break;
                case EXECUTION_END_TIME_FIELD:
                    executionEndTime = ParseUtils.toInstant(parser);
                    break;
                case ERROR_FIELD:
                    error = parser.text();
                    break;
                case ENTITY_FIELD:
                    entityList = new ArrayList<>();
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        entityList.add(Entity.parse(parser));
                    }
                    break;
                case USER_FIELD:
                    user = User.parse(parser);
                    break;
                case CommonName.SCHEMA_VERSION_FIELD:
                    schemaVersion = parser.intValue();
                    break;
                case TASK_ID_FIELD:
                    taskId = parser.text();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new AnomalyResult(
            detectorId,
            taskId,
            anomalyScore,
            anomalyGrade,
            confidence,
            featureData,
            dataStartTime,
            dataEndTime,
            executionStartTime,
            executionEndTime,
            error,
            entityList,
            user,
            schemaVersion
        );
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AnomalyResult that = (AnomalyResult) o;
        return Objects.equal(getDetectorId(), that.getDetectorId())
            && Objects.equal(getTaskId(), that.getTaskId())
            && Objects.equal(getAnomalyScore(), that.getAnomalyScore())
            && Objects.equal(getAnomalyGrade(), that.getAnomalyGrade())
            && Objects.equal(getConfidence(), that.getConfidence())
            && Objects.equal(getFeatureData(), that.getFeatureData())
            && Objects.equal(getDataStartTime(), that.getDataStartTime())
            && Objects.equal(getDataEndTime(), that.getDataEndTime())
            && Objects.equal(getExecutionStartTime(), that.getExecutionStartTime())
            && Objects.equal(getExecutionEndTime(), that.getExecutionEndTime())
            && Objects.equal(getError(), that.getError())
            && Objects.equal(getEntity(), that.getEntity());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects
            .hashCode(
                getDetectorId(),
                getTaskId(),
                getAnomalyScore(),
                getAnomalyGrade(),
                getConfidence(),
                getFeatureData(),
                getDataStartTime(),
                getDataEndTime(),
                getExecutionStartTime(),
                getExecutionEndTime(),
                getError(),
                getEntity()
            );
    }

    @Generated
    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("detectorId", detectorId)
            .append("taskId", taskId)
            .append("anomalyScore", anomalyScore)
            .append("anomalyGrade", anomalyGrade)
            .append("confidence", confidence)
            .append("featureData", featureData)
            .append("dataStartTime", dataStartTime)
            .append("dataEndTime", dataEndTime)
            .append("executionStartTime", executionStartTime)
            .append("executionEndTime", executionEndTime)
            .append("error", error)
            .append("entity", entity)
            .toString();
    }

    public String getDetectorId() {
        return detectorId;
    }

    public String getTaskId() {
        return taskId;
    }

    public Double getAnomalyScore() {
        return anomalyScore;
    }

    public Double getAnomalyGrade() {
        return anomalyGrade;
    }

    public Double getConfidence() {
        return confidence;
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

    public List<Entity> getEntity() {
        return entity;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(detectorId);
        out.writeDouble(anomalyScore);
        out.writeDouble(anomalyGrade);
        out.writeDouble(confidence);
        out.writeVInt(featureData.size());
        for (FeatureData feature : featureData) {
            feature.writeTo(out);
        }
        out.writeInstant(dataStartTime);
        out.writeInstant(dataEndTime);
        out.writeInstant(executionStartTime);
        out.writeInstant(executionEndTime);
        out.writeOptionalString(error);
        if (entity != null) {
            out.writeBoolean(true);
            out.writeVInt(entity.size());
            for (Entity entityItem : entity) {
                entityItem.writeTo(out);
            }
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
        out.writeOptionalString(taskId);
    }
}
