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

package org.opensearch.forecast.model;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.forecast.constant.ForecastCommonName.DUMMY_FORECASTER_ID;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.timeseries.annotation.Generated;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.constant.CommonValue;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.util.ParseUtils;

import com.google.common.base.Objects;

/**
 * Include result returned from RCF model and feature data.
 */
public class ForecastResult extends IndexableResult {
    public static final String PARSE_FIELD_NAME = "ForecastResult";
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
        ForecastResult.class,
        new ParseField(PARSE_FIELD_NAME),
        it -> parse(it)
    );

    public static final String FEATURE_ID_FIELD = "feature_id";
    public static final String VALUE_FIELD = "forecast_value";
    public static final String LOWER_BOUND_FIELD = "forecast_lower_bound";
    public static final String UPPER_BOUND_FIELD = "forecast_upper_bound";
    public static final String INTERVAL_WIDTH_FIELD = "confidence_interval_width";
    public static final String FORECAST_DATA_START_TIME_FIELD = "forecast_data_start_time";
    public static final String FORECAST_DATA_END_TIME_FIELD = "forecast_data_end_time";
    public static final String HORIZON_INDEX_FIELD = "horizon_index";

    private final String featureId;
    private final Float forecastValue;
    private final Float lowerBound;
    private final Float upperBound;
    private final Float confidenceIntervalWidth;
    private final Instant forecastDataStartTime;
    private final Instant forecastDataEndTime;
    private final Integer horizonIndex;
    protected final Double dataQuality;

    // used when indexing exception or error or an empty result
    public ForecastResult(
        String forecasterId,
        String taskId,
        List<FeatureData> featureData,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executionStartTime,
        Instant executionEndTime,
        String error,
        Optional<Entity> entity,
        User user,
        Integer schemaVersion,
        String modelId
    ) {
        this(
            forecasterId,
            taskId,
            Double.NaN,
            featureData,
            dataStartTime,
            dataEndTime,
            executionStartTime,
            executionEndTime,
            error,
            entity,
            user,
            schemaVersion,
            modelId,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    public ForecastResult(
        String forecasterId,
        String taskId,
        Double dataQuality,
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
        String featureId,
        Float forecastValue,
        Float lowerBound,
        Float upperBound,
        Instant forecastDataStartTime,
        Instant forecastDataEndTime,
        Integer horizonIndex
    ) {
        super(
            forecasterId,
            featureData,
            dataStartTime,
            dataEndTime,
            executionStartTime,
            executionEndTime,
            error,
            entity,
            user,
            schemaVersion,
            modelId,
            taskId
        );
        this.featureId = featureId;
        this.dataQuality = dataQuality;
        this.forecastValue = forecastValue;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.confidenceIntervalWidth = lowerBound != null && upperBound != null ? Math.abs(upperBound - lowerBound) : Float.NaN;
        this.forecastDataStartTime = forecastDataStartTime;
        this.forecastDataEndTime = forecastDataEndTime;
        this.horizonIndex = horizonIndex;
    }

    public static List<ForecastResult> fromRawRCFCasterResult(
        String forecasterId,
        long intervalMillis,
        Double dataQuality,
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
        float[] forecastsValues,
        float[] forecastsUppers,
        float[] forecastsLowers,
        String taskId
    ) {
        int inputLength = featureData.size();
        int numberOfForecasts = forecastsValues.length / inputLength;

        List<ForecastResult> convertedForecastValues = new ArrayList<>(numberOfForecasts);

        // store feature data and forecast value separately for easy query on feature data
        // we can join them using forecasterId, entityId, and executionStartTime/executionEndTime
        convertedForecastValues
            .add(
                new ForecastResult(
                    forecasterId,
                    taskId,
                    dataQuality,
                    featureData,
                    dataStartTime,
                    dataEndTime,
                    executionStartTime,
                    executionEndTime,
                    error,
                    entity,
                    user,
                    schemaVersion,
                    modelId,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    -1
                )
            );
        Instant forecastDataStartTime = dataEndTime;

        for (int i = 0; i < numberOfForecasts; i++) {
            Instant forecastDataEndTime = forecastDataStartTime.plusMillis(intervalMillis);
            for (int j = 0; j < inputLength; j++) {
                int k = i * inputLength + j;
                convertedForecastValues
                    .add(
                        new ForecastResult(
                            forecasterId,
                            taskId,
                            dataQuality,
                            null,
                            null,
                            null,
                            executionStartTime,
                            executionEndTime,
                            error,
                            entity,
                            user,
                            schemaVersion,
                            modelId,
                            featureData.get(j).getFeatureId(),
                            forecastsValues[k],
                            forecastsLowers[k],
                            forecastsUppers[k],
                            forecastDataStartTime,
                            forecastDataEndTime,
                            i
                        )
                    );
            }
            forecastDataStartTime = forecastDataEndTime;
        }

        return convertedForecastValues;
    }

    public ForecastResult(StreamInput input) throws IOException {
        super(input);
        this.featureId = input.readOptionalString();
        this.dataQuality = input.readOptionalDouble();
        this.forecastValue = input.readOptionalFloat();
        this.lowerBound = input.readOptionalFloat();
        this.upperBound = input.readOptionalFloat();
        this.confidenceIntervalWidth = input.readOptionalFloat();
        this.forecastDataStartTime = input.readOptionalInstant();
        this.forecastDataEndTime = input.readOptionalInstant();
        this.horizonIndex = input.readOptionalInt();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(ForecastCommonName.FORECASTER_ID_KEY, configId)
            .field(CommonName.SCHEMA_VERSION_FIELD, schemaVersion);

        if (dataStartTime != null) {
            xContentBuilder.field(CommonName.DATA_START_TIME_FIELD, dataStartTime.toEpochMilli());
        }
        if (dataEndTime != null) {
            xContentBuilder.field(CommonName.DATA_END_TIME_FIELD, dataEndTime.toEpochMilli());
        }
        if (featureData != null) {
            // can be null during preview
            xContentBuilder.field(CommonName.FEATURE_DATA_FIELD, featureData.toArray());
        }
        if (executionStartTime != null) {
            // can be null during preview
            xContentBuilder.field(CommonName.EXECUTION_START_TIME_FIELD, executionStartTime.toEpochMilli());
        }
        if (executionEndTime != null) {
            // can be null during preview
            xContentBuilder.field(CommonName.EXECUTION_END_TIME_FIELD, executionEndTime.toEpochMilli());
        }
        if (error != null) {
            xContentBuilder.field(CommonName.ERROR_FIELD, error);
        }
        if (optionalEntity.isPresent()) {
            xContentBuilder.field(CommonName.ENTITY_FIELD, optionalEntity.get());
        }
        if (user != null) {
            xContentBuilder.field(CommonName.USER_FIELD, user);
        }
        if (modelId != null) {
            xContentBuilder.field(CommonName.MODEL_ID_FIELD, modelId);
        }
        if (dataQuality != null && !dataQuality.isNaN()) {
            xContentBuilder.field(CommonName.DATA_QUALITY_FIELD, dataQuality);
        }
        if (taskId != null) {
            xContentBuilder.field(CommonName.TASK_ID_FIELD, taskId);
        }
        if (entityId != null) {
            xContentBuilder.field(CommonName.ENTITY_ID_FIELD, entityId);
        }
        if (forecastValue != null) {
            xContentBuilder.field(VALUE_FIELD, forecastValue);
        }
        if (lowerBound != null) {
            xContentBuilder.field(LOWER_BOUND_FIELD, lowerBound);
        }
        if (upperBound != null) {
            xContentBuilder.field(UPPER_BOUND_FIELD, upperBound);
        }
        if (forecastDataStartTime != null) {
            xContentBuilder.field(FORECAST_DATA_START_TIME_FIELD, forecastDataStartTime.toEpochMilli());
        }
        if (forecastDataEndTime != null) {
            xContentBuilder.field(FORECAST_DATA_END_TIME_FIELD, forecastDataEndTime.toEpochMilli());
        }
        if (horizonIndex != null) {
            xContentBuilder.field(HORIZON_INDEX_FIELD, horizonIndex);
        }
        if (featureId != null) {
            xContentBuilder.field(FEATURE_ID_FIELD, featureId);
        }

        return xContentBuilder.endObject();
    }

    public static ForecastResult parse(XContentParser parser) throws IOException {
        String forecasterId = null;
        Double dataQuality = null;
        List<FeatureData> featureData = null;
        Instant dataStartTime = null;
        Instant dataEndTime = null;
        Instant executionStartTime = null;
        Instant executionEndTime = null;
        String error = null;
        Entity entity = null;
        User user = null;
        Integer schemaVersion = CommonValue.NO_SCHEMA_VERSION;
        String modelId = null;
        String taskId = null;

        String featureId = null;
        Float forecastValue = null;
        Float lowerBound = null;
        Float upperBound = null;
        Instant forecastDataStartTime = null;
        Instant forecastDataEndTime = null;
        Integer horizonIndex = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case ForecastCommonName.FORECASTER_ID_KEY:
                    forecasterId = parser.text();
                    break;
                case CommonName.DATA_QUALITY_FIELD:
                    dataQuality = parser.doubleValue();
                    break;
                case CommonName.FEATURE_DATA_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    featureData = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        featureData.add(FeatureData.parse(parser));
                    }
                    break;
                case CommonName.DATA_START_TIME_FIELD:
                    dataStartTime = ParseUtils.toInstant(parser);
                    break;
                case CommonName.DATA_END_TIME_FIELD:
                    dataEndTime = ParseUtils.toInstant(parser);
                    break;
                case CommonName.EXECUTION_START_TIME_FIELD:
                    executionStartTime = ParseUtils.toInstant(parser);
                    break;
                case CommonName.EXECUTION_END_TIME_FIELD:
                    executionEndTime = ParseUtils.toInstant(parser);
                    break;
                case CommonName.ERROR_FIELD:
                    error = parser.text();
                    break;
                case CommonName.ENTITY_FIELD:
                    entity = Entity.parse(parser);
                    break;
                case CommonName.USER_FIELD:
                    user = User.parse(parser);
                    break;
                case CommonName.SCHEMA_VERSION_FIELD:
                    schemaVersion = parser.intValue();
                    break;
                case CommonName.MODEL_ID_FIELD:
                    modelId = parser.text();
                    break;
                case FEATURE_ID_FIELD:
                    featureId = parser.text();
                    break;
                case LOWER_BOUND_FIELD:
                    lowerBound = parser.floatValue();
                    break;
                case UPPER_BOUND_FIELD:
                    upperBound = parser.floatValue();
                    break;
                case VALUE_FIELD:
                    forecastValue = parser.floatValue();
                    break;
                case FORECAST_DATA_START_TIME_FIELD:
                    forecastDataStartTime = ParseUtils.toInstant(parser);
                    break;
                case FORECAST_DATA_END_TIME_FIELD:
                    forecastDataEndTime = ParseUtils.toInstant(parser);
                    break;
                case CommonName.TASK_ID_FIELD:
                    taskId = parser.text();
                    break;
                case HORIZON_INDEX_FIELD:
                    horizonIndex = parser.intValue();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }

        return new ForecastResult(
            forecasterId,
            taskId,
            dataQuality,
            featureData,
            dataStartTime,
            dataEndTime,
            executionStartTime,
            executionEndTime,
            error,
            Optional.ofNullable(entity),
            user,
            schemaVersion,
            modelId,
            featureId,
            forecastValue,
            lowerBound,
            upperBound,
            forecastDataStartTime,
            forecastDataEndTime,
            horizonIndex
        );
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        ForecastResult that = (ForecastResult) o;
        return Objects.equal(featureId, that.featureId)
            && Objects.equal(dataQuality, that.dataQuality)
            && Objects.equal(forecastValue, that.forecastValue)
            && Objects.equal(lowerBound, that.lowerBound)
            && Objects.equal(upperBound, that.upperBound)
            && Objects.equal(confidenceIntervalWidth, that.confidenceIntervalWidth)
            && Objects.equal(forecastDataStartTime, that.forecastDataStartTime)
            && Objects.equal(forecastDataEndTime, that.forecastDataEndTime)
            && Objects.equal(horizonIndex, that.horizonIndex);
    }

    @Generated
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects
            .hashCode(
                featureId,
                dataQuality,
                forecastValue,
                lowerBound,
                upperBound,
                confidenceIntervalWidth,
                forecastDataStartTime,
                forecastDataEndTime,
                horizonIndex
            );
        return result;
    }

    @Generated
    @Override
    public String toString() {
        return super.toString()
            + ", "
            + new ToStringBuilder(this)
                .append("featureId", featureId)
                .append("dataQuality", dataQuality)
                .append("forecastValue", forecastValue)
                .append("lowerBound", lowerBound)
                .append("upperBound", upperBound)
                .append("confidenceIntervalWidth", confidenceIntervalWidth)
                .append("forecastDataStartTime", forecastDataStartTime)
                .append("forecastDataEndTime", forecastDataEndTime)
                .append("horizonIndex", horizonIndex)
                .toString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeOptionalString(featureId);
        out.writeOptionalDouble(dataQuality);
        out.writeOptionalFloat(forecastValue);
        out.writeOptionalFloat(lowerBound);
        out.writeOptionalFloat(upperBound);
        out.writeOptionalFloat(confidenceIntervalWidth);
        out.writeOptionalInstant(forecastDataStartTime);
        out.writeOptionalInstant(forecastDataEndTime);
        out.writeOptionalInt(horizonIndex);
    }

    public static ForecastResult getDummyResult() {
        return new ForecastResult(
            DUMMY_FORECASTER_ID,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Optional.empty(),
            null,
            CommonValue.NO_SCHEMA_VERSION,
            null
        );
    }

    /**
     * Used to throw away requests when index pressure is high.
     * @return  when the error is there.
     */
    @Override
    public boolean isHighPriority() {
        // AnomalyResult.toXContent won't record Double.NaN and thus make it null
        return getError() != null;
    }

    public Double getDataQuality() {
        return dataQuality;
    }

    public String getFeatureId() {
        return featureId;
    }

    public Float getForecastValue() {
        return forecastValue;
    }

    public Float getLowerBound() {
        return lowerBound;
    }

    public Float getUpperBound() {
        return upperBound;
    }

    public Float getConfidenceIntervalWidth() {
        return confidenceIntervalWidth;
    }

    public Instant getForecastDataStartTime() {
        return forecastDataStartTime;
    }

    public Instant getForecastDataEndTime() {
        return forecastDataEndTime;
    }

    public Integer getHorizonIndex() {
        return horizonIndex;
    }
}
