/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.model;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.forecast.constant.ForecastCommonName.CUSTOM_RESULT_INDEX_PREFIX;
import static org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opensearch.common.ParsingException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.settings.ForecastNumericSetting;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.constant.CommonValue;
import org.opensearch.timeseries.dataprocessor.ImputationOption;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.TimeConfiguration;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.ParseUtils;

import com.google.common.base.Objects;

/**
 * Similar to AnomalyDetector, Forecaster defines config object. We cannot inherit from
 * AnomalyDetector as AnomalyDetector uses detection interval but Forecaster doesn't
 * need it and has to set it to null. Detection interval being null would fail
 * AnomalyDetector's constructor because detection interval cannot be null.
 */
public class Forecaster extends Config {
    public static final String FORECAST_PARSE_FIELD_NAME = "Forecaster";
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
        Forecaster.class,
        new ParseField(FORECAST_PARSE_FIELD_NAME),
        it -> parse(it)
    );

    public static final String HORIZON_FIELD = "horizon";
    public static final String FORECAST_INTERVAL_FIELD = "forecast_interval";
    public static final int DEFAULT_HORIZON_SHINGLE_RATIO = 3;

    private Integer horizon;

    public Forecaster(
        String forecasterId,
        Long version,
        String name,
        String description,
        String timeField,
        List<String> indices,
        List<Feature> features,
        QueryBuilder filterQuery,
        TimeConfiguration forecastInterval,
        TimeConfiguration windowDelay,
        Integer shingleSize,
        Map<String, Object> uiMetadata,
        Integer schemaVersion,
        Instant lastUpdateTime,
        List<String> categoryFields,
        User user,
        String resultIndex,
        Integer horizon,
        ImputationOption imputationOption
    ) {
        super(
            forecasterId,
            version,
            name,
            description,
            timeField,
            indices,
            features,
            filterQuery,
            windowDelay,
            shingleSize,
            uiMetadata,
            schemaVersion,
            lastUpdateTime,
            categoryFields,
            user,
            resultIndex,
            forecastInterval,
            imputationOption
        );
        if (forecastInterval == null) {
            errorMessage = ForecastCommonMessages.NULL_FORECAST_INTERVAL;
            issueType = ValidationIssueType.FORECAST_INTERVAL;
        } else if (((IntervalTimeConfiguration) forecastInterval).getInterval() <= 0) {
            errorMessage = ForecastCommonMessages.INVALID_FORECAST_INTERVAL;
            issueType = ValidationIssueType.FORECAST_INTERVAL;
        }

        int maxCategoryFields = ForecastNumericSetting.maxCategoricalFields();
        if (categoryFields != null && categoryFields.size() > maxCategoryFields) {
            errorMessage = CommonMessages.getTooManyCategoricalFieldErr(maxCategoryFields);
            issueType = ValidationIssueType.CATEGORY;
        }

        if (errorMessage != null && issueType != null) {
            throw new ValidationException(errorMessage, issueType, ValidationAspect.FORECASTER);
        } else if (errorMessage != null || issueType != null) {
            throw new TimeSeriesException(CommonMessages.FAIL_TO_VALIDATE);
        }

        if (invalidHorizon(horizon)) {
            throw new ValidationException(
                "Horizon size must be a positive integer no larger than "
                    + TimeSeriesSettings.MAX_SHINGLE_SIZE * DEFAULT_HORIZON_SHINGLE_RATIO
                    + ". Got "
                    + horizon,
                ValidationIssueType.SHINGLE_SIZE_FIELD,
                ValidationAspect.FORECASTER
            );
        }
        this.horizon = horizon;
    }

    public Forecaster(StreamInput input) throws IOException {
        super(input);
        horizon = input.readInt();
    }

    @Override
    public void writeTo(StreamOutput output) throws IOException {
        super.writeTo(output);
        output.writeInt(horizon);
    }

    public boolean invalidHorizon(Integer horizonToTest) {
        return horizonToTest != null
            && (horizonToTest < 1 || horizonToTest > TimeSeriesSettings.MAX_SHINGLE_SIZE * DEFAULT_HORIZON_SHINGLE_RATIO);
    }

    /**
     * Parse raw json content into forecaster instance.
     *
     * @param parser json based content parser
     * @return forecaster instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static Forecaster parse(XContentParser parser) throws IOException {
        return parse(parser, null);
    }

    public static Forecaster parse(XContentParser parser, String forecasterId) throws IOException {
        return parse(parser, forecasterId, null);
    }

    /**
     * Parse raw json content and given forecaster id into forecaster instance.
     *
     * @param parser     json based content parser
     * @param forecasterId forecaster id
     * @param version    forecaster document version
     * @return forecaster instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static Forecaster parse(XContentParser parser, String forecasterId, Long version) throws IOException {
        return parse(parser, forecasterId, version, null, null);
    }

    /**
     * Parse raw json content and given forecaster id into forecaster instance.
     *
     * @param parser                      json based content parser
     * @param forecasterId                forecaster id
     * @param version                     forecast document version
     * @param defaultForecastInterval     default forecaster interval
     * @param defaultForecastWindowDelay  default forecaster window delay
     * @return forecaster instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static Forecaster parse(
        XContentParser parser,
        String forecasterId,
        Long version,
        TimeValue defaultForecastInterval,
        TimeValue defaultForecastWindowDelay
    ) throws IOException {
        String name = null;
        String description = "";
        String timeField = null;
        List<String> indices = new ArrayList<String>();
        QueryBuilder filterQuery = QueryBuilders.matchAllQuery();
        TimeConfiguration forecastInterval = defaultForecastInterval == null
            ? null
            : new IntervalTimeConfiguration(defaultForecastInterval.getMinutes(), ChronoUnit.MINUTES);
        TimeConfiguration windowDelay = defaultForecastWindowDelay == null
            ? null
            : new IntervalTimeConfiguration(defaultForecastWindowDelay.getSeconds(), ChronoUnit.SECONDS);
        Integer shingleSize = null;
        List<Feature> features = new ArrayList<>();
        Integer schemaVersion = CommonValue.NO_SCHEMA_VERSION;
        Map<String, Object> uiMetadata = null;
        Instant lastUpdateTime = null;
        User user = null;
        String resultIndex = null;

        List<String> categoryField = null;
        Integer horizon = null;
        ImputationOption interpolationOption = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case NAME_FIELD:
                    name = parser.text();
                    break;
                case DESCRIPTION_FIELD:
                    description = parser.text();
                    break;
                case TIMEFIELD_FIELD:
                    timeField = parser.text();
                    break;
                case INDICES_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        indices.add(parser.text());
                    }
                    break;
                case UI_METADATA_FIELD:
                    uiMetadata = parser.map();
                    break;
                case CommonName.SCHEMA_VERSION_FIELD:
                    schemaVersion = parser.intValue();
                    break;
                case FILTER_QUERY_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    try {
                        filterQuery = parseInnerQueryBuilder(parser);
                    } catch (ParsingException | XContentParseException e) {
                        throw new ValidationException(
                            "Custom query error in data filter: " + e.getMessage(),
                            ValidationIssueType.FILTER_QUERY,
                            ValidationAspect.DETECTOR
                        );
                    } catch (IllegalArgumentException e) {
                        if (!e.getMessage().contains("empty clause")) {
                            throw e;
                        }
                    }
                    break;
                case FORECAST_INTERVAL_FIELD:
                    try {
                        forecastInterval = TimeConfiguration.parse(parser);
                    } catch (Exception e) {
                        if (e instanceof IllegalArgumentException && e.getMessage().contains(CommonMessages.NEGATIVE_TIME_CONFIGURATION)) {
                            throw new ValidationException(
                                "Detection interval must be a positive integer",
                                ValidationIssueType.FORECAST_INTERVAL,
                                ValidationAspect.DETECTOR
                            );
                        }
                        throw e;
                    }
                    break;
                case FEATURE_ATTRIBUTES_FIELD:
                    try {
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            features.add(Feature.parse(parser));
                        }
                    } catch (Exception e) {
                        if (e instanceof ParsingException || e instanceof XContentParseException) {
                            throw new ValidationException(
                                "Custom query error: " + e.getMessage(),
                                ValidationIssueType.FEATURE_ATTRIBUTES,
                                ValidationAspect.DETECTOR
                            );
                        }
                        throw e;
                    }
                    break;
                case WINDOW_DELAY_FIELD:
                    try {
                        windowDelay = TimeConfiguration.parse(parser);
                    } catch (Exception e) {
                        if (e instanceof IllegalArgumentException && e.getMessage().contains(CommonMessages.NEGATIVE_TIME_CONFIGURATION)) {
                            throw new ValidationException(
                                "Window delay interval must be a positive integer",
                                ValidationIssueType.WINDOW_DELAY,
                                ValidationAspect.DETECTOR
                            );
                        }
                        throw e;
                    }
                    break;
                case SHINGLE_SIZE_FIELD:
                    shingleSize = parser.intValue();
                    break;
                case LAST_UPDATE_TIME_FIELD:
                    lastUpdateTime = ParseUtils.toInstant(parser);
                    break;
                case CATEGORY_FIELD:
                    categoryField = (List) parser.list();
                    break;
                case USER_FIELD:
                    user = User.parse(parser);
                    break;
                case RESULT_INDEX_FIELD:
                    resultIndex = parser.text();
                    break;
                case HORIZON_FIELD:
                    horizon = parser.intValue();
                    break;
                case IMPUTATION_OPTION_FIELD:
                    interpolationOption = ImputationOption.parse(parser);
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        Forecaster forecaster = new Forecaster(
            forecasterId,
            version,
            name,
            description,
            timeField,
            indices,
            features,
            filterQuery,
            forecastInterval,
            windowDelay,
            getShingleSize(shingleSize),
            uiMetadata,
            schemaVersion,
            lastUpdateTime,
            categoryField,
            user,
            resultIndex,
            horizon,
            interpolationOption
        );
        return forecaster;
    }

    // TODO: test if this method works
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder = super.toXContent(xContentBuilder, params);
        xContentBuilder.field(FORECAST_INTERVAL_FIELD, interval).field(HORIZON_FIELD, horizon);

        return xContentBuilder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Forecaster forecaster = (Forecaster) o;
        return super.equals(o) && Objects.equal(horizon, forecaster.horizon);
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        hash = 89 * hash + (this.horizon != null ? this.horizon.hashCode() : 0);
        return hash;
    }

    @Override
    public String validateCustomResultIndex(String resultIndex) {
        if (resultIndex != null && !resultIndex.startsWith(CUSTOM_RESULT_INDEX_PREFIX)) {
            return ForecastCommonMessages.INVALID_RESULT_INDEX_PREFIX;
        }
        return super.validateCustomResultIndex(resultIndex);
    }

    @Override
    protected ValidationAspect getConfigValidationAspect() {
        return ValidationAspect.FORECASTER;
    }

    public Integer getHorizon() {
        return horizon;
    }
}
