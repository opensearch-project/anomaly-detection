/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.model;

import static org.opensearch.timeseries.constant.CommonMessages.INVALID_CHAR_IN_RESULT_INDEX_NAME;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.timeseries.annotation.Generated;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.dataprocessor.ImputationMethod;
import org.opensearch.timeseries.dataprocessor.ImputationOption;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.owasp.encoder.Encode;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

public abstract class Config implements Writeable, ToXContentObject {
    private static final Logger logger = LogManager.getLogger(Config.class);

    public static final int MAX_RESULT_INDEX_NAME_SIZE = 255;
    // OS doesn’t allow uppercase: https://tinyurl.com/yse2xdbx
    public static final String RESULT_INDEX_NAME_PATTERN = "[a-z0-9_-]+";

    public static final String NO_ID = "";
    public static final String TIMEOUT = "timeout";
    public static final String GENERAL_SETTINGS = "general_settings";
    public static final String AGGREGATION = "aggregation_issue";

    // field in JSON representation
    public static final String NAME_FIELD = "name";
    public static final String DESCRIPTION_FIELD = "description";
    public static final String TIMEFIELD_FIELD = "time_field";
    public static final String INDICES_FIELD = "indices";
    public static final String UI_METADATA_FIELD = "ui_metadata";
    public static final String FILTER_QUERY_FIELD = "filter_query";
    public static final String FEATURE_ATTRIBUTES_FIELD = "feature_attributes";
    public static final String WINDOW_DELAY_FIELD = "window_delay";
    public static final String SHINGLE_SIZE_FIELD = "shingle_size";
    public static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    public static final String CATEGORY_FIELD = "category_field";
    public static final String USER_FIELD = "user";
    public static final String RESULT_INDEX_FIELD = "result_index";
    public static final String IMPUTATION_OPTION_FIELD = "imputation_option";
    public static final String SEASONALITY_FIELD = "suggested_seasonality";
    public static final String RECENCY_EMPHASIS_FIELD = "recency_emphasis";
    public static final String HISTORY_INTERVAL_FIELD = "history";
    public static final String RESULT_INDEX_FIELD_MIN_SIZE = "result_index_min_size";
    public static final String RESULT_INDEX_FIELD_MIN_AGE = "result_index_min_age";
    public static final String RESULT_INDEX_FIELD_TTL = "result_index_ttl";
    public static final String FLATTEN_RESULT_INDEX_MAPPING = "flatten_result_index_mapping";
    // Changing categorical field, feature attributes, interval, windowDelay, time field, horizon, indices,
    // result index would force us to display results only from the most recent update. Otherwise,
    // the UI appear cluttered and unclear.
    // We cannot use last update time as it would change whenever other fields like name changes.
    public static final String BREAKING_UI_CHANGE_TIME = "last_ui_breaking_change_time";

    protected String id;
    protected Long version;
    protected String name;
    protected String description;
    protected String timeField;
    protected List<String> indices;
    protected List<Feature> featureAttributes;
    protected QueryBuilder filterQuery;
    protected TimeConfiguration interval;
    protected TimeConfiguration windowDelay;
    protected Integer shingleSize;
    protected String customResultIndexOrAlias;
    protected Map<String, Object> uiMetadata;
    protected Integer schemaVersion;
    protected Instant lastUpdateTime;
    protected List<String> categoryFields;
    protected User user;
    protected ImputationOption imputationOption;
    // Aggregation period to smooth the emphasis on the most recent data. Aggregation period to smooth
    // the emphasis of the most recent data. Useful for determining short/long term trends. Can be used
    // similar to moving average computation https://en.wikipedia.org/wiki/Moving_average
    // Recency emphasis is the average number of steps that a point will be included in the sample.
    // Call the number of steps that a point is included in the sample the "lifetime" of the point
    // (which may be 0). Over a finite time window, the distribution of the lifetime of a point is
    // approximately exponential with parameter lambda. In an exponential distribution, the average
    // is the reciprocal of the rate parameter (λ). Thus, 1 / timmeDecay is approximately the
    // average number of steps that a point will be included in the sample.
    protected Integer recencyEmphasis;

    // validation error
    protected String errorMessage;
    protected ValidationIssueType issueType;

    protected Integer seasonIntervals;
    protected Integer historyIntervals;
    protected Integer customResultIndexMinSize;
    protected Integer customResultIndexMinAge;
    protected Integer customResultIndexTTL;
    protected Boolean flattenResultIndexMapping;
    protected Instant lastUIBreakingChangeTime;

    public static String INVALID_RESULT_INDEX_NAME_SIZE = "Result index name size must contains less than "
        + MAX_RESULT_INDEX_NAME_SIZE
        + " characters";

    protected Config(
        String id,
        Long version,
        String name,
        String description,
        String timeField,
        List<String> indices,
        List<Feature> features,
        QueryBuilder filterQuery,
        TimeConfiguration windowDelay,
        Integer shingleSize,
        Map<String, Object> uiMetadata,
        Integer schemaVersion,
        Instant lastUpdateTime,
        List<String> categoryFields,
        User user,
        String resultIndex,
        TimeConfiguration interval,
        ImputationOption imputationOption,
        Integer recencyEmphasis,
        Integer seasonIntervals,
        ShingleGetter shingleGetter,
        Integer historyIntervals,
        Integer customResultIndexMinSize,
        Integer customResultIndexMinAge,
        Integer customResultIndexTTL,
        Boolean flattenResultIndexMapping,
        Instant lastBreakingUIChangeTime
    ) {
        if (Strings.isBlank(name)) {
            errorMessage = CommonMessages.EMPTY_NAME;
            issueType = ValidationIssueType.NAME;
            return;
        }
        if (Strings.isBlank(timeField)) {
            errorMessage = CommonMessages.NULL_TIME_FIELD;
            issueType = ValidationIssueType.TIMEFIELD_FIELD;
            return;
        }
        if (indices == null || indices.isEmpty()) {
            errorMessage = CommonMessages.EMPTY_INDICES;
            issueType = ValidationIssueType.INDICES;
            return;
        }

        if (invalidShingleSizeRange(shingleSize)) {
            errorMessage = "Shingle size must be a positive integer no larger than "
                + TimeSeriesSettings.MAX_SHINGLE_SIZE
                + ". Got "
                + shingleSize;
            issueType = ValidationIssueType.SHINGLE_SIZE_FIELD;
            return;
        }

        if (invalidSeasonality(seasonIntervals)) {
            errorMessage = "Suggested seasonality must be a positive integer no larger than "
                + TimeSeriesSettings.MAX_SHINGLE_SIZE * 2
                + ". Got "
                + seasonIntervals;
            issueType = ValidationIssueType.SUGGESTED_SEASONALITY_FIELD;
            return;
        }

        errorMessage = validateCustomResultIndex(resultIndex);
        if (errorMessage != null) {
            issueType = ValidationIssueType.RESULT_INDEX;
            return;
        }

        if (recencyEmphasis != null && recencyEmphasis <= 1) {
            issueType = ValidationIssueType.RECENCY_EMPHASIS;
            errorMessage = "Recency emphasis must be an integer greater than 1.";
            return;
        }

        errorMessage = validateDescription(description);
        if (errorMessage != null) {
            issueType = ValidationIssueType.DESCRIPTION;
            return;
        }

        if (historyIntervals != null && (historyIntervals <= 0 || historyIntervals > TimeSeriesSettings.MAX_HISTORY_INTERVALS)) {
            issueType = ValidationIssueType.HISTORY;
            errorMessage = "We cannot look back more than " + TimeSeriesSettings.MAX_HISTORY_INTERVALS + " intervals.";
            return;
        }

        List<String> redundantNames = findRedundantNames(features);
        if (redundantNames.size() > 0) {
            issueType = ValidationIssueType.FEATURE_ATTRIBUTES;
            errorMessage = redundantNames + " appears more than once. Feature name has to be unique";
            return;
        }

        if (imputationOption != null && imputationOption.getMethod() == ImputationMethod.FIXED_VALUES) {
            // Calculate the number of enabled features
            List<Feature> enabledFeatures = features == null
                ? null
                : features.stream().filter(Feature::getEnabled).collect(Collectors.toList());

            Map<String, Double> defaultFill = imputationOption.getDefaultFill();

            // Case 1: enabledFeatures == null && defaultFill != null
            if (enabledFeatures == null && defaultFill != null && !defaultFill.isEmpty()) {
                issueType = ValidationIssueType.IMPUTATION;
                errorMessage = "Enabled features list is null, but default fill values are provided.";
                return;
            }

            // Case 2: enabledFeatures != null && defaultFill == null
            if (enabledFeatures != null && (defaultFill == null || defaultFill.isEmpty())) {
                issueType = ValidationIssueType.IMPUTATION;
                errorMessage = "Enabled features are present, but no default fill values are provided.";
                return;
            }

            // Case 3: enabledFeatures.size() != defaultFill.size()
            if (enabledFeatures != null && defaultFill != null && defaultFill.size() != enabledFeatures.size()) {
                issueType = ValidationIssueType.IMPUTATION;
                errorMessage = String
                    .format(
                        Locale.ROOT,
                        "Mismatch between the number of enabled features and default fill values. Number of default fill values: %d. Number of enabled features: %d.",
                        defaultFill.size(),
                        enabledFeatures.size()
                    );
                return;
            }

            for (int i = 0; i < enabledFeatures.size(); i++) {
                if (!defaultFill.containsKey(enabledFeatures.get(i).getName())) {
                    issueType = ValidationIssueType.IMPUTATION;
                    errorMessage = String.format(Locale.ROOT, "Missing feature name: %s.", enabledFeatures.get(i).getName());
                    return;
                }
            }
        }

        this.id = id;
        this.version = version;
        this.name = name;
        this.description = description;
        this.timeField = timeField;
        this.indices = indices;
        // we validate empty or no enabled features when starting config (Read IndexJobActionHandler.validateConfig)
        this.featureAttributes = features == null ? ImmutableList.of() : ImmutableList.copyOf(features);
        this.filterQuery = filterQuery;
        this.interval = interval;
        this.windowDelay = windowDelay;
        this.shingleSize = shingleGetter.getShingleSize(shingleSize);
        this.uiMetadata = uiMetadata;
        this.schemaVersion = schemaVersion;
        this.lastUpdateTime = lastUpdateTime;
        this.categoryFields = categoryFields;
        this.user = user;
        this.customResultIndexOrAlias = Strings.trimToNull(resultIndex);
        this.imputationOption = imputationOption;
        this.issueType = null;
        this.errorMessage = null;
        // If recencyEmphasis is null, use the default value from TimeSeriesSettings
        this.recencyEmphasis = Optional.ofNullable(recencyEmphasis).orElse(TimeSeriesSettings.DEFAULT_RECENCY_EMPHASIS);
        this.seasonIntervals = seasonIntervals;
        this.historyIntervals = historyIntervals == null ? suggestHistory() : historyIntervals;
        this.customResultIndexMinSize = Strings.trimToNull(resultIndex) == null ? null : customResultIndexMinSize;
        this.customResultIndexMinAge = Strings.trimToNull(resultIndex) == null ? null : customResultIndexMinAge;
        this.customResultIndexTTL = Strings.trimToNull(resultIndex) == null ? null : customResultIndexTTL;
        this.flattenResultIndexMapping = Strings.trimToNull(resultIndex) == null ? null : flattenResultIndexMapping;
        this.lastUIBreakingChangeTime = lastBreakingUIChangeTime;
    }

    public int suggestHistory() {
        return TimeSeriesSettings.NUM_MIN_SAMPLES + this.shingleSize;
    }

    public Config(StreamInput input) throws IOException {
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
            this.uiMetadata = input.readMap();
        } else {
            this.uiMetadata = null;
        }
        customResultIndexOrAlias = input.readOptionalString();
        if (input.readBoolean()) {
            this.imputationOption = new ImputationOption(input);
        } else {
            this.imputationOption = null;
        }
        this.recencyEmphasis = input.readInt();
        this.seasonIntervals = input.readOptionalInt();
        this.historyIntervals = input.readInt();
        this.customResultIndexMinSize = input.readOptionalInt();
        this.customResultIndexMinAge = input.readOptionalInt();
        this.customResultIndexTTL = input.readOptionalInt();
        this.flattenResultIndexMapping = input.readOptionalBoolean();
        this.lastUIBreakingChangeTime = input.readOptionalInstant();
    }

    /*
     * Implicit constructor that be called implicitly when a subtype
     * needs to call like AnomalyDetector(StreamInput). Otherwise,
     * we will have compiler error:
     * "Implicit super constructor Config() is undefined.
     * Must explicitly invoke another constructor".
     */
    public Config() {}

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
        if (uiMetadata != null) {
            output.writeBoolean(true);
            output.writeMap(uiMetadata);
        } else {
            output.writeBoolean(false);
        }
        output.writeOptionalString(customResultIndexOrAlias);
        if (imputationOption != null) {
            output.writeBoolean(true);
            imputationOption.writeTo(output);
        } else {
            output.writeBoolean(false);
        }
        output.writeInt(recencyEmphasis);
        output.writeOptionalInt(seasonIntervals);
        output.writeInt(historyIntervals);
        output.writeOptionalInt(customResultIndexMinSize);
        output.writeOptionalInt(customResultIndexMinAge);
        output.writeOptionalInt(customResultIndexTTL);
        output.writeOptionalBoolean(flattenResultIndexMapping);
        output.writeOptionalInstant(lastUIBreakingChangeTime);
    }

    public boolean invalidShingleSizeRange(Integer shingleSizeToTest) {
        return shingleSizeToTest != null && (shingleSizeToTest < 1 || shingleSizeToTest > TimeSeriesSettings.MAX_SHINGLE_SIZE);
    }

    public boolean invalidSeasonality(Integer seasonalityToTest) {
        if (seasonalityToTest == null) {
            return false;
        }
        // shingle size = suggested seasonality / 2
        // given seasonality, we can reuse shingle size verification
        // cannot be smaller than 1
        return invalidShingleSizeRange(Math.max(1, seasonalityToTest / TimeSeriesSettings.SEASONALITY_TO_SHINGLE_RATIO));
    }

    /**
     *
     * @return either ValidationAspect.FORECASTER or ValidationAspect.DETECTOR
     *  depending on this is a forecaster or detector config.
     */
    protected abstract ValidationAspect getConfigValidationAspect();

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Config config = (Config) o;
        // a few fields not included:
        // 1)didn't include uiMetadata since toXContent/parse will produce a map of map
        // and cause the parsed one not equal to the original one. This can be confusing.
        // 2)didn't include id, schemaVersion, and lastUpdateTime as we deemed equality based on contents.
        // Including id fails tests like AnomalyDetectorExecutionInput.testParseAnomalyDetectorExecutionInput.
        return Objects.equal(name, config.name)
            && Objects.equal(description, config.description)
            && Objects.equal(timeField, config.timeField)
            && Objects.equal(indices, config.indices)
            && Objects.equal(featureAttributes, config.featureAttributes)
            && Objects.equal(filterQuery, config.filterQuery)
            && Objects.equal(interval, config.interval)
            && Objects.equal(windowDelay, config.windowDelay)
            && Objects.equal(shingleSize, config.shingleSize)
            && Objects.equal(categoryFields, config.categoryFields)
            && Objects.equal(user, config.user)
            && Objects.equal(customResultIndexOrAlias, config.customResultIndexOrAlias)
            && Objects.equal(imputationOption, config.imputationOption)
            && Objects.equal(recencyEmphasis, config.recencyEmphasis)
            && Objects.equal(seasonIntervals, config.seasonIntervals)
            && Objects.equal(historyIntervals, config.historyIntervals)
            && Objects.equal(customResultIndexMinSize, config.customResultIndexMinSize)
            && Objects.equal(customResultIndexMinAge, config.customResultIndexMinAge)
            && Objects.equal(customResultIndexTTL, config.customResultIndexTTL)
            && Objects.equal(flattenResultIndexMapping, config.flattenResultIndexMapping);
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects
            .hashCode(
                name,
                description,
                timeField,
                indices,
                featureAttributes,
                filterQuery,
                interval,
                windowDelay,
                shingleSize,
                categoryFields,
                schemaVersion,
                user,
                customResultIndexOrAlias,
                imputationOption,
                recencyEmphasis,
                seasonIntervals,
                historyIntervals,
                customResultIndexMinSize,
                customResultIndexMinAge,
                customResultIndexTTL,
                flattenResultIndexMapping
            );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder
            .field(NAME_FIELD, name)
            .field(DESCRIPTION_FIELD, Encode.forHtml(description))
            .field(TIMEFIELD_FIELD, timeField)
            .field(INDICES_FIELD, indices.toArray())
            .field(FILTER_QUERY_FIELD, filterQuery)
            .field(WINDOW_DELAY_FIELD, windowDelay)
            .field(SHINGLE_SIZE_FIELD, shingleSize)
            .field(CommonName.SCHEMA_VERSION_FIELD, schemaVersion)
            .field(FEATURE_ATTRIBUTES_FIELD, featureAttributes.toArray())
            .field(RECENCY_EMPHASIS_FIELD, recencyEmphasis)
            .field(HISTORY_INTERVAL_FIELD, historyIntervals);

        if (uiMetadata != null && !uiMetadata.isEmpty()) {
            builder.field(UI_METADATA_FIELD, uiMetadata);
        }
        if (lastUpdateTime != null) {
            builder.field(LAST_UPDATE_TIME_FIELD, lastUpdateTime.toEpochMilli());
        }
        if (categoryFields != null) {
            builder.field(CATEGORY_FIELD, categoryFields.toArray());
        }
        if (user != null) {
            builder.field(USER_FIELD, user);
        }
        if (customResultIndexOrAlias != null) {
            builder.field(RESULT_INDEX_FIELD, customResultIndexOrAlias);
        }
        if (imputationOption != null) {
            builder.field(IMPUTATION_OPTION_FIELD, imputationOption);
        }
        if (seasonIntervals != null) {
            builder.field(SEASONALITY_FIELD, seasonIntervals);
        }
        if (customResultIndexMinSize != null) {
            builder.field(RESULT_INDEX_FIELD_MIN_SIZE, customResultIndexMinSize);
        }
        if (customResultIndexMinAge != null) {
            builder.field(RESULT_INDEX_FIELD_MIN_AGE, customResultIndexMinAge);
        }
        if (customResultIndexTTL != null) {
            builder.field(RESULT_INDEX_FIELD_TTL, customResultIndexTTL);
        }
        if (flattenResultIndexMapping != null) {
            builder.field(FLATTEN_RESULT_INDEX_MAPPING, flattenResultIndexMapping);
        }
        if (lastUIBreakingChangeTime != null) {
            builder.field(BREAKING_UI_CHANGE_TIME, lastUIBreakingChangeTime.toEpochMilli());
        }
        return builder;
    }

    public Long getVersion() {
        return version;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getTimeField() {
        return timeField;
    }

    public List<String> getIndices() {
        return indices;
    }

    public List<Feature> getFeatureAttributes() {
        return featureAttributes;
    }

    public QueryBuilder getFilterQuery() {
        return filterQuery;
    }

    /**
     * Returns enabled feature ids in the same order in feature attributes.
     *
     * @return a list of filtered feature ids.
     */
    public List<String> getEnabledFeatureIds() {
        return featureAttributes.stream().filter(Feature::getEnabled).map(Feature::getId).collect(Collectors.toList());
    }

    public List<String> getEnabledFeatureNames() {
        return featureAttributes.stream().filter(Feature::getEnabled).map(Feature::getName).collect(Collectors.toList());
    }

    public TimeConfiguration getInterval() {
        return interval;
    }

    public TimeConfiguration getWindowDelay() {
        return windowDelay;
    }

    public Integer getShingleSize() {
        return shingleSize;
    }

    public Map<String, Object> getUiMetadata() {
        return uiMetadata;
    }

    public Integer getSchemaVersion() {
        return schemaVersion;
    }

    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    public List<String> getCategoryFields() {
        return this.categoryFields;
    }

    public String getId() {
        return id;
    }

    public long getIntervalInMilliseconds() {
        return ((IntervalTimeConfiguration) getInterval()).toDuration().toMillis();
    }

    public long getIntervalInSeconds() {
        return getIntervalInMilliseconds() / 1000;
    }

    public long getIntervalInMinutes() {
        return getIntervalInMilliseconds() / 1000 / 60;
    }

    public Duration getIntervalDuration() {
        return ((IntervalTimeConfiguration) getInterval()).toDuration();
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    /**
     * Since 2.15, custom result index is changed to an alias to ease rollover as rollover target can only be an alias or data stream.
     * @return custom result index name or alias
     */
    public String getCustomResultIndexOrAlias() {
        return customResultIndexOrAlias;
    }

    public String getCustomResultIndexPattern() {
        return Strings.isEmpty(customResultIndexOrAlias) ? null : IndexManagement.getAllCustomResultIndexPattern(customResultIndexOrAlias);
    }

    public boolean isHighCardinality() {
        return Config.isHC(getCategoryFields());
    }

    public boolean hasMultipleCategories() {
        return categoryFields != null && categoryFields.size() > 1;
    }

    public String validateCustomResultIndex(String resultIndex) {
        if (resultIndex == null) {
            return null;
        }
        if (resultIndex.length() > MAX_RESULT_INDEX_NAME_SIZE) {
            return Config.INVALID_RESULT_INDEX_NAME_SIZE;
        }
        if (!resultIndex.matches(RESULT_INDEX_NAME_PATTERN)) {
            return INVALID_CHAR_IN_RESULT_INDEX_NAME;
        }
        return null;
    }

    public String validateDescription(String description) {
        if (Strings.isEmpty(description)) {
            return null;
        }
        if (description.length() > TimeSeriesSettings.MAX_DESCRIPTION_LENGTH) {
            return CommonMessages.DESCRIPTION_LENGTH_TOO_LONG;
        }
        return null;
    }

    public static boolean isHC(List<String> categoryFields) {
        return categoryFields != null && categoryFields.size() > 0;
    }

    public ImputationOption getImputationOption() {
        return imputationOption;
    }

    /**
     * Retrieves the transform decay value.
     *
     * This method implements an inverse relationship between the recency emphasis and the transform decay value,
     * such that the transform decay is set to 1 / recency emphasis. For example, a transform decay of 0.02
     * implies a recency emphasis of 50 observations (1/0.02).
     *
     * The transform decay value is crucial in determining the rate at which older data loses its influence in the model.
     *
     * @return The current transform decay value, dictating the rate of exponential decay in the model.
     */
    public Double getTimeDecay() {
        return 1.0 / recencyEmphasis;
    }

    protected void checkAndThrowValidationErrors(ValidationAspect validationAspect) {
        if (errorMessage != null && issueType != null) {
            throw new ValidationException(errorMessage, issueType, validationAspect);
        } else if (errorMessage != null || issueType != null) {
            throw new TimeSeriesException(CommonMessages.FAIL_TO_VALIDATE);
        }
    }

    public static Config parseConfig(Class<? extends Config> configClass, XContentParser parser) throws IOException {
        if (configClass == AnomalyDetector.class) {
            return AnomalyDetector.parse(parser);
        } else if (configClass == Forecaster.class) {
            return Forecaster.parse(parser);
        } else {
            throw new IllegalArgumentException("Unsupported config type. Supported config types are [AnomalyDetector, Forecaster]");
        }
    }

    public Integer getSeasonIntervals() {
        return seasonIntervals;
    }

    public Integer getRecencyEmphasis() {
        return recencyEmphasis;
    }

    public Integer getHistoryIntervals() {
        return historyIntervals;
    }

    public Integer getCustomResultIndexMinSize() {
        return customResultIndexMinSize;
    }

    public Integer getCustomResultIndexMinAge() {
        return customResultIndexMinAge;
    }

    public Integer getCustomResultIndexTTL() {
        return customResultIndexTTL;
    }

    public Boolean getFlattenResultIndexMapping() {
        return flattenResultIndexMapping;
    }

    public Instant getLastBreakingUIChangeTime() {
        return lastUIBreakingChangeTime;
    }

    /**
     * Identifies redundant feature names.
     *
     * @param features the list of features to check
     * @return a list of redundant feature names
     */
    public static List<String> findRedundantNames(List<Feature> features) {
        if (features == null || features.isEmpty()) {
            return new ArrayList<>();
        }

        // Group features by name and count occurrences
        Map<String, Long> nameCounts = features
            .stream()
            .map(Feature::getName)
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        // Filter names that appear more than once and collect them into a list
        List<String> redundantNames = nameCounts
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue() > 1)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

        return redundantNames;
    }

    @Generated
    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("name", name)
            .append("description", description)
            .append("timeField", timeField)
            .append("indices", indices)
            .append("featureAttributes", featureAttributes)
            .append("filterQuery", filterQuery)
            .append("interval", interval)
            .append("windowDelay", windowDelay)
            .append("shingleSize", shingleSize)
            .append("categoryFields", categoryFields)
            .append("schemaVersion", schemaVersion)
            .append("user", user)
            .append("customResultIndex", customResultIndexOrAlias)
            .append("imputationOption", imputationOption)
            .append("recencyEmphasis", recencyEmphasis)
            .append("seasonIntervals", seasonIntervals)
            .append("historyIntervals", historyIntervals)
            .append("customResultIndexMinSize", customResultIndexMinSize)
            .append("customResultIndexMinAge", customResultIndexMinAge)
            .append("customResultIndexTTL", customResultIndexTTL)
            .append("flattenResultIndexMapping", flattenResultIndexMapping)
            .toString();
    }
}
