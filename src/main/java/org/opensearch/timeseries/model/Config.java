/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.model;

import static org.opensearch.timeseries.constant.CommonMessages.INVALID_CHAR_IN_RESULT_INDEX_NAME;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.timeseries.annotation.Generated;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.dataprocessor.FixedValueImputer;
import org.opensearch.timeseries.dataprocessor.ImputationMethod;
import org.opensearch.timeseries.dataprocessor.ImputationOption;
import org.opensearch.timeseries.dataprocessor.Imputer;
import org.opensearch.timeseries.dataprocessor.LinearUniformImputer;
import org.opensearch.timeseries.dataprocessor.PreviousValueImputer;
import org.opensearch.timeseries.dataprocessor.ZeroImputer;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

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

    private static final Imputer zeroImputer;
    private static final Imputer previousImputer;
    private static final Imputer linearImputer;
    private static final Imputer linearImputerIntegerSensitive;

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
    protected String customResultIndex;
    protected Map<String, Object> uiMetadata;
    protected Integer schemaVersion;
    protected Instant lastUpdateTime;
    protected List<String> categoryFields;
    protected User user;
    protected ImputationOption imputationOption;

    // validation error
    protected String errorMessage;
    protected ValidationIssueType issueType;

    protected Imputer imputer;

    public static String INVALID_RESULT_INDEX_NAME_SIZE = "Result index name size must contains less than "
        + MAX_RESULT_INDEX_NAME_SIZE
        + " characters";

    static {
        zeroImputer = new ZeroImputer();
        previousImputer = new PreviousValueImputer();
        linearImputer = new LinearUniformImputer(false);
        linearImputerIntegerSensitive = new LinearUniformImputer(true);
    }

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
        ImputationOption imputationOption
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

        errorMessage = validateCustomResultIndex(resultIndex);
        if (errorMessage != null) {
            issueType = ValidationIssueType.RESULT_INDEX;
            return;
        }

        if (imputationOption != null
            && imputationOption.getMethod() == ImputationMethod.FIXED_VALUES
            && imputationOption.getDefaultFill().isEmpty()) {
            issueType = ValidationIssueType.IMPUTATION;
            errorMessage = "No given values for fixed value interpolation";
            return;
        }

        this.id = id;
        this.version = version;
        this.name = name;
        this.description = description;
        this.timeField = timeField;
        this.indices = indices;
        this.featureAttributes = features == null ? ImmutableList.of() : ImmutableList.copyOf(features);
        this.filterQuery = filterQuery;
        this.interval = interval;
        this.windowDelay = windowDelay;
        this.shingleSize = getShingleSize(shingleSize);
        this.uiMetadata = uiMetadata;
        this.schemaVersion = schemaVersion;
        this.lastUpdateTime = lastUpdateTime;
        this.categoryFields = categoryFields;
        this.user = user;
        this.customResultIndex = Strings.trimToNull(resultIndex);
        this.imputationOption = imputationOption;
        this.imputer = createImputer();
        this.issueType = null;
        this.errorMessage = null;
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
        customResultIndex = input.readOptionalString();
        if (input.readBoolean()) {
            this.imputationOption = new ImputationOption(input);
        } else {
            this.imputationOption = null;
        }
        this.imputer = createImputer();
    }

    /*
     * Implicit constructor that be called implicitly when a subtype
     * needs to call like AnomalyDetector(StreamInput). Otherwise,
     * we will have compiler error:
     * "Implicit super constructor Config() is undefined.
     * Must explicitly invoke another constructor".
     */
    public Config() {
        this.imputer = null;
    }

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
        output.writeOptionalString(customResultIndex);
        if (imputationOption != null) {
            output.writeBoolean(true);
            imputationOption.writeTo(output);
        } else {
            output.writeBoolean(false);
        }
    }

    /**
     * If the given shingle size is null, return default;
     * otherwise, return the given shingle size.
     *
     * @param customShingleSize Given shingle size
     * @return Shingle size
     */
    protected static Integer getShingleSize(Integer customShingleSize) {
        return customShingleSize == null ? TimeSeriesSettings.DEFAULT_SHINGLE_SIZE : customShingleSize;
    }

    public boolean invalidShingleSizeRange(Integer shingleSizeToTest) {
        return shingleSizeToTest != null && (shingleSizeToTest < 1 || shingleSizeToTest > TimeSeriesSettings.MAX_SHINGLE_SIZE);
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
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
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
            && Objects.equal(customResultIndex, config.customResultIndex)
            && Objects.equal(imputationOption, config.imputationOption);
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
                customResultIndex,
                imputationOption
            );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder
            .field(NAME_FIELD, name)
            .field(DESCRIPTION_FIELD, description)
            .field(TIMEFIELD_FIELD, timeField)
            .field(INDICES_FIELD, indices.toArray())
            .field(FILTER_QUERY_FIELD, filterQuery)
            .field(WINDOW_DELAY_FIELD, windowDelay)
            .field(SHINGLE_SIZE_FIELD, shingleSize)
            .field(CommonName.SCHEMA_VERSION_FIELD, schemaVersion)
            .field(FEATURE_ATTRIBUTES_FIELD, featureAttributes.toArray());

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
        if (customResultIndex != null) {
            builder.field(RESULT_INDEX_FIELD, customResultIndex);
        }
        if (imputationOption != null) {
            builder.field(IMPUTATION_OPTION_FIELD, imputationOption);
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

    public String getCustomResultIndex() {
        return customResultIndex;
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

    public static boolean isHC(List<String> categoryFields) {
        return categoryFields != null && categoryFields.size() > 0;
    }

    public ImputationOption getImputationOption() {
        return imputationOption;
    }

    public Imputer getImputer() {
        if (imputer != null) {
            return imputer;
        }
        imputer = createImputer();
        return imputer;
    }

    protected Imputer createImputer() {
        Imputer imputer = null;

        // default interpolator is using last known value
        if (imputationOption == null) {
            return previousImputer;
        }

        switch (imputationOption.getMethod()) {
            case ZERO:
                imputer = zeroImputer;
                break;
            case FIXED_VALUES:
                // we did validate default fill is not empty in the constructor
                imputer = new FixedValueImputer(imputationOption.getDefaultFill().get());
                break;
            case PREVIOUS:
                imputer = previousImputer;
                break;
            case LINEAR:
                if (imputationOption.isIntegerSentive()) {
                    imputer = linearImputerIntegerSensitive;
                } else {
                    imputer = linearImputer;
                }
                break;
            default:
                logger.error("unsupported method: " + imputationOption.getMethod());
                imputer = new PreviousValueImputer();
                break;
        }
        return imputer;
    }

    protected void checkAndThrowValidationErrors(ValidationAspect validationAspect) {
        if (errorMessage != null && issueType != null) {
            throw new ValidationException(errorMessage, issueType, validationAspect);
        } else if (errorMessage != null || issueType != null) {
            throw new TimeSeriesException(CommonMessages.FAIL_TO_VALIDATE);
        }
    }
}
