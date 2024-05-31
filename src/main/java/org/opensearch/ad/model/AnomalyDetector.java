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

import static org.opensearch.ad.constant.ADCommonName.CUSTOM_RESULT_INDEX_PREFIX;
import static org.opensearch.ad.model.AnomalyDetectorType.MULTI_ENTITY;
import static org.opensearch.ad.model.AnomalyDetectorType.SINGLE_ENTITY;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.settings.ADNumericSetting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.timeseries.annotation.Generated;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonValue;
import org.opensearch.timeseries.dataprocessor.ImputationOption;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.ShingleGetter;
import org.opensearch.timeseries.model.TimeConfiguration;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.ParseUtils;

import com.google.common.base.Objects;

/**
 * An AnomalyDetector is used to represent anomaly detection model(RCF) related parameters.
 * NOTE: If change detector config index mapping, you should change AD task index mapping as well.
 * TODO: Will replace detector config mapping in AD task with detector config setting directly \
 *      in code rather than config it in anomaly-detection-state.json file.
 */
public class AnomalyDetector extends Config {
    static class ADShingleGetter implements ShingleGetter {
        private Integer seasonIntervals;

        public ADShingleGetter(Integer seasonIntervals) {
            this.seasonIntervals = seasonIntervals;
        }

        /**
         * If the given shingle size not null, return given shingle size;
         * if seasonality not null, return seasonality hint / 2
         * otherwise, return default shingle size.
         *
         * @param customShingleSize Given shingle size
         * @return Shingle size
         */
        @Override
        public Integer getShingleSize(Integer customShingleSize) {
            if (customShingleSize != null) {
                return customShingleSize;
            }

            if (seasonIntervals != null) {
                return seasonIntervals / TimeSeriesSettings.SEASONALITY_TO_SHINGLE_RATIO;
            }

            return TimeSeriesSettings.DEFAULT_SHINGLE_SIZE;
        }
    }

    public static final String PARSE_FIELD_NAME = "AnomalyDetector";
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
        AnomalyDetector.class,
        new ParseField(PARSE_FIELD_NAME),
        it -> parse(it)
    );
    public static final String TYPE = "_doc";
    // for bwc, we have to keep this field instead of reusing an interval field in the super class.
    // otherwise, we won't be able to recognize "detection_interval" field sent from old implementation.
    public static final String DETECTION_INTERVAL_FIELD = "detection_interval";
    public static final String DETECTOR_TYPE_FIELD = "detector_type";
    @Deprecated
    public static final String DETECTION_DATE_RANGE_FIELD = "detection_date_range";
    public static final String RULES_FIELD = "rules";

    protected String detectorType;

    // TODO: support backward compatibility, will remove in future
    @Deprecated
    private DateRange detectionDateRange;

    public static String INVALID_RESULT_INDEX_NAME_SIZE = "Result index name size must contains less than "
        + MAX_RESULT_INDEX_NAME_SIZE
        + " characters";

    private List<Rule> rules;

    /**
     * Constructor function.
     *
     * @param detectorId        detector identifier
     * @param version           detector document version
     * @param name              detector name
     * @param description       description of detector
     * @param timeField         time field
     * @param indices           indices used as detector input
     * @param features          detector feature attributes
     * @param filterQuery       detector filter query
     * @param detectionInterval detecting interval
     * @param windowDelay       max delay window for realtime data
     * @param shingleSize       number of the most recent time intervals to form a shingled data point
     * @param uiMetadata        metadata used by OpenSearch-Dashboards
     * @param schemaVersion     anomaly detector index mapping version
     * @param lastUpdateTime    detector's last update time
     * @param categoryFields    a list of partition fields
     * @param user              user to which detector is associated
     * @param resultIndex       result index
     * @param imputationOption interpolation method and optional default values
     * @param recencyEmphasis Aggregation period to smooth the emphasis on the most recent data.
     * @param seasonIntervals seasonality in terms of intervals
     * @param historyIntervals history intervals we look back during cold start
     * @param rules custom rules to filter out AD results
     * @param customResultIndexMinSize custom result index lifecycle management min size condition
     * @param customResultIndexMinAge custom result index lifecycle management min age condition
     * @param customResultIndexTTL custom result index lifecycle management ttl
     */
    public AnomalyDetector(
        String detectorId,
        Long version,
        String name,
        String description,
        String timeField,
        List<String> indices,
        List<Feature> features,
        QueryBuilder filterQuery,
        TimeConfiguration detectionInterval,
        TimeConfiguration windowDelay,
        Integer shingleSize,
        Map<String, Object> uiMetadata,
        Integer schemaVersion,
        Instant lastUpdateTime,
        List<String> categoryFields,
        User user,
        String resultIndex,
        ImputationOption imputationOption,
        Integer recencyEmphasis,
        Integer seasonIntervals,
        Integer historyIntervals,
        List<Rule> rules,
        Integer customResultIndexMinSize,
        Integer customResultIndexMinAge,
        Integer customResultIndexTTL
    ) {
        super(
            detectorId,
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
            detectionInterval,
            imputationOption,
            recencyEmphasis,
            seasonIntervals,
            new ADShingleGetter(seasonIntervals),
            historyIntervals,
            customResultIndexMinSize,
            customResultIndexMinAge,
            customResultIndexTTL
        );

        checkAndThrowValidationErrors(ValidationAspect.DETECTOR);

        if (detectionInterval == null) {
            errorMessage = ADCommonMessages.NULL_DETECTION_INTERVAL;
            issueType = ValidationIssueType.DETECTION_INTERVAL;
        } else if (((IntervalTimeConfiguration) detectionInterval).getInterval() <= 0) {
            errorMessage = ADCommonMessages.INVALID_DETECTION_INTERVAL;
            issueType = ValidationIssueType.DETECTION_INTERVAL;
        }

        int maxCategoryFields = ADNumericSetting.maxCategoricalFields();
        if (categoryFields != null && categoryFields.size() > maxCategoryFields) {
            errorMessage = CommonMessages.getTooManyCategoricalFieldErr(maxCategoryFields);
            issueType = ValidationIssueType.CATEGORY;
        }

        checkAndThrowValidationErrors(ValidationAspect.DETECTOR);

        this.detectorType = isHC(categoryFields) ? MULTI_ENTITY.name() : SINGLE_ENTITY.name();

        this.rules = rules == null ? getDefaultRule() : rules;
    }

    /*
     * For backward compatiblity reason, we cannot use super class
     * Config's constructor as we have detectionDateRange and
     * detectorType that Config does not have.
     */
    public AnomalyDetector(StreamInput input) throws IOException {
        id = input.readOptionalString();
        version = input.readOptionalLong();
        name = input.readString();
        description = input.readOptionalString();
        timeField = input.readString();
        indices = input.readStringList();
        featureAttributes = input.readList(Feature::new);
        filterQuery = input.readNamedWriteable(QueryBuilder.class);
        interval = IntervalTimeConfiguration.readFrom(input);
        windowDelay = IntervalTimeConfiguration.readFrom(input);
        shingleSize = input.readInt();
        schemaVersion = input.readInt();
        this.categoryFields = input.readOptionalStringList();
        lastUpdateTime = input.readInstant();
        if (input.readBoolean()) {
            this.user = new User(input);
        } else {
            user = null;
        }
        if (input.readBoolean()) {
            detectionDateRange = new DateRange(input);
        } else {
            detectionDateRange = null;
        }
        detectorType = input.readOptionalString();
        if (input.readBoolean()) {
            this.uiMetadata = input.readMap();
        } else {
            this.uiMetadata = null;
        }
        customResultIndex = input.readOptionalString();
        if (input.readBoolean()) {
            this.imputationOption = new ImputationOption(input);
        } else {
            this.imputationOption = null;
        }
        this.recencyEmphasis = input.readInt();
        this.seasonIntervals = input.readInt();
        this.historyIntervals = input.readInt();
        if (input.readBoolean()) {
            this.rules = input.readList(Rule::new);
        }
        if (input.readBoolean()) {
            this.customResultIndexMinSize = input.readInt();
        }
        if (input.readBoolean()) {
            this.customResultIndexMinAge = input.readInt();
        }
        if (input.readBoolean()) {
            this.customResultIndexTTL = input.readInt();
        }
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    /*
     * For backward compatibility reason, we cannot use super class
     * Config's writeTo as we have detectionDateRange and
     * detectorType that Config does not have.
     */
    @Override
    public void writeTo(StreamOutput output) throws IOException {
        output.writeOptionalString(id);
        output.writeOptionalLong(version);
        output.writeString(name);
        output.writeOptionalString(description);
        output.writeString(timeField);
        output.writeStringCollection(indices);
        output.writeList(featureAttributes);
        output.writeNamedWriteable(filterQuery);
        interval.writeTo(output);
        windowDelay.writeTo(output);
        output.writeInt(shingleSize);
        output.writeInt(schemaVersion);
        output.writeOptionalStringCollection(categoryFields);
        output.writeInstant(lastUpdateTime);
        if (user != null) {
            output.writeBoolean(true); // user exists
            user.writeTo(output);
        } else {
            output.writeBoolean(false); // user does not exist
        }
        if (detectionDateRange != null) {
            output.writeBoolean(true); // detectionDateRange exists
            detectionDateRange.writeTo(output);
        } else {
            output.writeBoolean(false); // detectionDateRange does not exist
        }
        output.writeOptionalString(detectorType);
        if (uiMetadata != null) {
            output.writeBoolean(true);
            output.writeMap(uiMetadata);
        } else {
            output.writeBoolean(false);
        }
        output.writeOptionalString(customResultIndex);
        if (imputationOption != null) {
            output.writeBoolean(true);
            imputationOption.writeTo(output);
        } else {
            output.writeBoolean(false);
        }
        output.writeInt(recencyEmphasis);
        output.writeInt(seasonIntervals);
        output.writeInt(historyIntervals);
        if (rules != null) {
            output.writeBoolean(true);
            output.writeList(rules);
        } else {
            output.writeBoolean(false);
        }
        if (customResultIndexMinSize != null) {
            output.writeOptionalInt(customResultIndexMinSize);
        } else {
            output.writeBoolean(false);
        }
        if (customResultIndexMinSize != null) {
            output.writeOptionalInt(customResultIndexMinAge);
        } else {
            output.writeBoolean(false);
        }
        if (customResultIndexMinSize != null) {
            output.writeOptionalInt(customResultIndexTTL);
        } else {
            output.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder = super.toXContent(xContentBuilder, params);
        xContentBuilder.field(DETECTION_INTERVAL_FIELD, interval);

        if (detectorType != null) {
            xContentBuilder.field(DETECTOR_TYPE_FIELD, detectorType);
        }
        if (detectionDateRange != null) {
            xContentBuilder.field(DETECTION_DATE_RANGE_FIELD, detectionDateRange);
        }
        if (rules != null) {
            xContentBuilder.field(RULES_FIELD, rules.toArray());
        }

        return xContentBuilder.endObject();
    }

    /**
     * Parse raw json content into anomaly detector instance.
     *
     * @param parser json based content parser
     * @return anomaly detector instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AnomalyDetector parse(XContentParser parser) throws IOException {
        return parse(parser, null);
    }

    public static AnomalyDetector parse(XContentParser parser, String detectorId) throws IOException {
        return parse(parser, detectorId, null);
    }

    /**
     * Parse raw json content and given detector id into anomaly detector instance.
     *
     * @param parser     json based content parser
     * @param detectorId detector id
     * @param version    detector document version
     * @return anomaly detector instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AnomalyDetector parse(XContentParser parser, String detectorId, Long version) throws IOException {
        return parse(parser, detectorId, version, null, null);
    }

    /**
     * Parse raw json content and given detector id into anomaly detector instance.
     *
     * @param parser                      json based content parser
     * @param detectorId                  detector id
     * @param version                     detector document version
     * @param defaultDetectionInterval    default detection interval
     * @param defaultDetectionWindowDelay default detection window delay
     * @return anomaly detector instance
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AnomalyDetector parse(
        XContentParser parser,
        String detectorId,
        Long version,
        TimeValue defaultDetectionInterval,
        TimeValue defaultDetectionWindowDelay
    ) throws IOException {
        String name = null;
        String description = "";
        String timeField = null;
        List<String> indices = new ArrayList<String>();
        QueryBuilder filterQuery = QueryBuilders.matchAllQuery();
        TimeConfiguration detectionInterval = defaultDetectionInterval == null
            ? null
            : new IntervalTimeConfiguration(defaultDetectionInterval.getMinutes(), ChronoUnit.MINUTES);
        TimeConfiguration windowDelay = defaultDetectionWindowDelay == null
            ? null
            : new IntervalTimeConfiguration(defaultDetectionWindowDelay.getSeconds(), ChronoUnit.SECONDS);
        Integer shingleSize = null;
        List<Feature> features = new ArrayList<>();
        Integer schemaVersion = CommonValue.NO_SCHEMA_VERSION;
        Map<String, Object> uiMetadata = null;
        Instant lastUpdateTime = null;
        User user = null;
        DateRange detectionDateRange = null;
        String resultIndex = null;

        List<String> categoryField = null;
        ImputationOption imputationOption = null;
        Integer recencyEmphasis = null;
        Integer seasonality = null;
        Integer historyIntervals = null;
        List<Rule> rules = new ArrayList<>();
        Integer customResultIndexMinSize = null;
        Integer customResultIndexMinAge = null;
        Integer customResultIndexTTL = null;

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
                case org.opensearch.timeseries.constant.CommonName.SCHEMA_VERSION_FIELD:
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
                case DETECTION_INTERVAL_FIELD:
                    try {
                        detectionInterval = TimeConfiguration.parse(parser);
                    } catch (Exception e) {
                        if (e instanceof IllegalArgumentException && e.getMessage().contains(CommonMessages.NEGATIVE_TIME_CONFIGURATION)) {
                            throw new ValidationException(
                                "Detection interval must be a positive integer",
                                ValidationIssueType.DETECTION_INTERVAL,
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
                case DETECTION_DATE_RANGE_FIELD:
                    detectionDateRange = DateRange.parse(parser);
                    break;
                case RESULT_INDEX_FIELD:
                    resultIndex = parser.text();
                    break;
                case IMPUTATION_OPTION_FIELD:
                    imputationOption = ImputationOption.parse(parser);
                    break;
                case RECENCY_EMPHASIS_FIELD:
                    recencyEmphasis = parser.intValue();
                    break;
                case SEASONALITY_FIELD:
                    seasonality = parser.currentToken() == XContentParser.Token.VALUE_NULL ? null : parser.intValue();
                    break;
                case HISTORY_INTERVAL_FIELD:
                    historyIntervals = parser.intValue();
                    break;
                case RULES_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        rules.add(Rule.parse(parser));
                    }
                    break;
                case RESULT_INDEX_FIELD_MIN_SIZE:
                    customResultIndexMinSize = parser.intValue();
                    break;
                case RESULT_INDEX_FIELD_MIN_AGE:
                    customResultIndexMinAge = parser.intValue();
                    break;
                case RESULT_INDEX_FIELD_TTL:
                    customResultIndexTTL = parser.intValue();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        AnomalyDetector detector = new AnomalyDetector(
            detectorId,
            version,
            name,
            description,
            timeField,
            indices,
            features,
            filterQuery,
            detectionInterval,
            windowDelay,
            shingleSize,
            uiMetadata,
            schemaVersion,
            lastUpdateTime,
            categoryField,
            user,
            resultIndex,
            imputationOption,
            recencyEmphasis,
            seasonality,
            historyIntervals,
            rules,
            customResultIndexMinSize,
            customResultIndexMinAge,
            customResultIndexTTL
        );
        detector.setDetectionDateRange(detectionDateRange);
        return detector;
    }

    public String getDetectorType() {
        return detectorType;
    }

    public void setDetectionDateRange(DateRange detectionDateRange) {
        this.detectionDateRange = detectionDateRange;
    }

    public DateRange getDetectionDateRange() {
        return detectionDateRange;
    }

    public List<Rule> getRules() {
        return rules;
    }

    @Override
    protected ValidationAspect getConfigValidationAspect() {
        return ValidationAspect.DETECTOR;
    }

    @Override
    public String validateCustomResultIndex(String resultIndex) {
        if (resultIndex != null && !resultIndex.startsWith(CUSTOM_RESULT_INDEX_PREFIX)) {
            return ADCommonMessages.INVALID_RESULT_INDEX_PREFIX;
        }
        return super.validateCustomResultIndex(resultIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AnomalyDetector detector = (AnomalyDetector) o;
        return super.equals(o) && Objects.equal(rules, detector.rules);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hashCode(rules);
        return result;
    }

    @Generated
    @Override
    public String toString() {
        return super.toString() + ", " + new ToStringBuilder(this).append("rules", rules).toString();
    }

    private List<Rule> getDefaultRule() {
        List<Rule> rules = new ArrayList<>();
        for (Feature feature : featureAttributes) {
            if (feature.getEnabled()) {
                rules
                    .add(
                        new Rule(
                            Action.IGNORE_ANOMALY,
                            Arrays
                                .asList(
                                    new Condition(feature.getName(), ThresholdType.ACTUAL_OVER_EXPECTED_RATIO, Operator.LTE, 0.2),
                                    new Condition(feature.getName(), ThresholdType.EXPECTED_OVER_ACTUAL_RATIO, Operator.LTE, 0.2)
                                )
                        )
                    );
            }
        }
        return rules;
    }
}
