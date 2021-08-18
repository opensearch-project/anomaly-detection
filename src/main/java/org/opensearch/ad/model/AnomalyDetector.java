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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.DEFAULT_MULTI_ENTITY_SHINGLE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.DEFAULT_SHINGLE_SIZE;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.util.Strings;
import org.opensearch.ad.annotation.Generated;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.NumericSetting;
import org.opensearch.ad.util.ParseUtils;
import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.commons.authuser.User;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

/**
 * An AnomalyDetector is used to represent anomaly detection model(RCF) related parameters.
 * NOTE: If change detector config index mapping, you should change AD task index mapping as well.
 * TODO: Will replace detector config mapping in AD task with detector config setting directly \
 *      in code rather than config it in anomaly-detection-state.json file.
 */
public class AnomalyDetector implements Writeable, ToXContentObject {

    public static final String PARSE_FIELD_NAME = "AnomalyDetector";
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
        AnomalyDetector.class,
        new ParseField(PARSE_FIELD_NAME),
        it -> parse(it)
    );
    public static final String NO_ID = "";
    public static final String ANOMALY_DETECTORS_INDEX = ".opendistro-anomaly-detectors";
    public static final String TYPE = "_doc";
    public static final String QUERY_PARAM_PERIOD_START = "period_start";
    public static final String QUERY_PARAM_PERIOD_END = "period_end";

    private static final String NAME_FIELD = "name";
    private static final String DESCRIPTION_FIELD = "description";
    private static final String TIMEFIELD_FIELD = "time_field";
    private static final String INDICES_FIELD = "indices";
    private static final String FILTER_QUERY_FIELD = "filter_query";
    private static final String FEATURE_ATTRIBUTES_FIELD = "feature_attributes";
    private static final String DETECTION_INTERVAL_FIELD = "detection_interval";
    private static final String WINDOW_DELAY_FIELD = "window_delay";
    private static final String SHINGLE_SIZE_FIELD = "shingle_size";
    private static final String LAST_UPDATE_TIME_FIELD = "last_update_time";
    public static final String UI_METADATA_FIELD = "ui_metadata";
    public static final String CATEGORY_FIELD = "category_field";
    public static final String USER_FIELD = "user";
    public static final String DETECTOR_TYPE_FIELD = "detector_type";
    @Deprecated
    public static final String DETECTION_DATE_RANGE_FIELD = "detection_date_range";

    private final String detectorId;
    private final Long version;
    private final String name;
    private final String description;
    private final String timeField;
    private final List<String> indices;
    private final List<Feature> featureAttributes;
    private final QueryBuilder filterQuery;
    private final TimeConfiguration detectionInterval;
    private final TimeConfiguration windowDelay;
    private final Integer shingleSize;
    private final Map<String, Object> uiMetadata;
    private final Integer schemaVersion;
    private final Instant lastUpdateTime;
    private final List<String> categoryFields;
    private User user;
    private String detectorType;

    // TODO: support backward compatibility, will remove in future
    @Deprecated
    private DetectionDateRange detectionDateRange;

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
     * @param uiMetadata        metadata used by Kibana
     * @param schemaVersion     anomaly detector index mapping version
     * @param lastUpdateTime    detector's last update time
     * @param categoryFields    a list of partition fields
     * @param user              user to which detector is associated
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
        User user
    ) {
        this(
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
            categoryFields,
            user,
            null
        );
    }

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
        String detectorType
    ) {
        if (Strings.isBlank(name)) {
            throw new IllegalArgumentException("Detector name should be set");
        }
        if (timeField == null) {
            throw new IllegalArgumentException("Time field should be set");
        }
        if (indices == null || indices.isEmpty()) {
            throw new IllegalArgumentException("Indices should be set");
        }
        if (detectionInterval == null) {
            throw new IllegalArgumentException("Detection interval should be set");
        }
        if (invalidShingleSizeRange(shingleSize)) {
            throw new IllegalArgumentException(
                "Shingle size must be a positive integer no larger than "
                    + AnomalyDetectorSettings.MAX_SHINGLE_SIZE
                    + ". Got "
                    + shingleSize
            );
        }
        int maxCategoryFields = NumericSetting.maxCategoricalFields();
        if (categoryFields != null && categoryFields.size() > maxCategoryFields) {
            throw new IllegalArgumentException(CommonErrorMessages.getTooManyCategoricalFieldErr(maxCategoryFields));
        }
        if (((IntervalTimeConfiguration) detectionInterval).getInterval() <= 0) {
            throw new IllegalArgumentException("Detection interval must be a positive integer");
        }
        this.detectorId = detectorId;
        this.version = version;
        this.name = name;
        this.description = description;
        this.timeField = timeField;
        this.indices = indices;
        this.featureAttributes = features == null ? ImmutableList.of() : ImmutableList.copyOf(features);
        this.filterQuery = filterQuery;
        this.detectionInterval = detectionInterval;
        this.windowDelay = windowDelay;
        this.shingleSize = getShingleSize(shingleSize, categoryFields);
        this.uiMetadata = uiMetadata;
        this.schemaVersion = schemaVersion;
        this.lastUpdateTime = lastUpdateTime;
        this.categoryFields = categoryFields;
        this.user = user;
        this.detectorType = detectorType;
    }

    public AnomalyDetector(StreamInput input) throws IOException {
        detectorId = input.readOptionalString();
        version = input.readOptionalLong();
        name = input.readString();
        description = input.readOptionalString();
        timeField = input.readString();
        indices = input.readStringList();
        featureAttributes = input.readList(Feature::new);
        filterQuery = input.readNamedWriteable(QueryBuilder.class);
        detectionInterval = IntervalTimeConfiguration.readFrom(input);
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
            detectionDateRange = new DetectionDateRange(input);
        } else {
            detectionDateRange = null;
        }
        detectorType = input.readOptionalString();
        if (input.readBoolean()) {
            this.uiMetadata = input.readMap();
        } else {
            this.uiMetadata = null;
        }
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    @Override
    public void writeTo(StreamOutput output) throws IOException {
        output.writeOptionalString(detectorId);
        output.writeOptionalLong(version);
        output.writeString(name);
        output.writeOptionalString(description);
        output.writeString(timeField);
        output.writeStringCollection(indices);
        output.writeList(featureAttributes);
        output.writeNamedWriteable(filterQuery);
        detectionInterval.writeTo(output);
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
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(NAME_FIELD, name)
            .field(DESCRIPTION_FIELD, description)
            .field(TIMEFIELD_FIELD, timeField)
            .field(INDICES_FIELD, indices.toArray())
            .field(FILTER_QUERY_FIELD, filterQuery)
            .field(DETECTION_INTERVAL_FIELD, detectionInterval)
            .field(WINDOW_DELAY_FIELD, windowDelay)
            .field(SHINGLE_SIZE_FIELD, shingleSize)
            .field(CommonName.SCHEMA_VERSION_FIELD, schemaVersion)
            .field(FEATURE_ATTRIBUTES_FIELD, featureAttributes.toArray());

        if (uiMetadata != null && !uiMetadata.isEmpty()) {
            xContentBuilder.field(UI_METADATA_FIELD, uiMetadata);
        }
        if (lastUpdateTime != null) {
            xContentBuilder.field(LAST_UPDATE_TIME_FIELD, lastUpdateTime.toEpochMilli());
        }
        if (categoryFields != null) {
            xContentBuilder.field(CATEGORY_FIELD, categoryFields.toArray());
        }
        if (user != null) {
            xContentBuilder.field(USER_FIELD, user);
        }
        if (detectorType != null) {
            xContentBuilder.field(DETECTOR_TYPE_FIELD, detectorType);
        }
        if (detectionDateRange != null) {
            xContentBuilder.field(DETECTION_DATE_RANGE_FIELD, detectionDateRange);
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
        String description = null;
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
        DetectionDateRange detectionDateRange = null;

        List<String> categoryField = null;

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
                    } catch (IllegalArgumentException e) {
                        if (!e.getMessage().contains("empty clause")) {
                            throw e;
                        }
                    }
                    break;
                case DETECTION_INTERVAL_FIELD:
                    detectionInterval = TimeConfiguration.parse(parser);
                    break;
                case FEATURE_ATTRIBUTES_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        features.add(Feature.parse(parser));
                    }
                    break;
                case WINDOW_DELAY_FIELD:
                    windowDelay = TimeConfiguration.parse(parser);
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
                    detectionDateRange = DetectionDateRange.parse(parser);
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
            getShingleSize(shingleSize, categoryField),
            uiMetadata,
            schemaVersion,
            lastUpdateTime,
            categoryField,
            user
        );
        detector.setDetectionDateRange(detectionDateRange);
        return detector;
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AnomalyDetector detector = (AnomalyDetector) o;
        return Objects.equal(getName(), detector.getName())
            && Objects.equal(getDescription(), detector.getDescription())
            && Objects.equal(getTimeField(), detector.getTimeField())
            && Objects.equal(getIndices(), detector.getIndices())
            && Objects.equal(getFeatureAttributes(), detector.getFeatureAttributes())
            && Objects.equal(getFilterQuery(), detector.getFilterQuery())
            && Objects.equal(getDetectionInterval(), detector.getDetectionInterval())
            && Objects.equal(getWindowDelay(), detector.getWindowDelay())
            && Objects.equal(getShingleSize(), detector.getShingleSize())
            && Objects.equal(getCategoryField(), detector.getCategoryField())
            && Objects.equal(getUser(), detector.getUser());
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects
            .hashCode(
                detectorId,
                name,
                description,
                timeField,
                indices,
                featureAttributes,
                detectionInterval,
                windowDelay,
                shingleSize,
                uiMetadata,
                schemaVersion,
                lastUpdateTime,
                user,
                detectorType
            );
    }

    public String getDetectorId() {
        return detectorId;
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

    public TimeConfiguration getDetectionInterval() {
        return detectionInterval;
    }

    public TimeConfiguration getWindowDelay() {
        return windowDelay;
    }

    public Integer getShingleSize() {
        return isMultientityDetector() ? DEFAULT_MULTI_ENTITY_SHINGLE : shingleSize;
    }

    /**
     * If the given shingle size is null, return default based on the kind of detector;
     * otherwise, return the given shingle size.
     *
     * TODO: need to deal with the case where customers start with single-entity detector, we set it to 8 by default;
     * then cx update it to multi-entity detector, we would still use 8 in this case.  Kibana needs to change to
     * give the correct shingle size.
     * @param customShingleSize Given shingle size
     * @param categoryField Used to verify if this is a multi-entity or single-entity detector
     * @return Shingle size
     */
    private static Integer getShingleSize(Integer customShingleSize, List<String> categoryField) {
        return customShingleSize == null
            ? (categoryField != null && categoryField.size() > 0 ? DEFAULT_MULTI_ENTITY_SHINGLE : DEFAULT_SHINGLE_SIZE)
            : customShingleSize;
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

    public List<String> getCategoryField() {
        return this.categoryFields;
    }

    public long getDetectorIntervalInMilliseconds() {
        return ((IntervalTimeConfiguration) getDetectionInterval()).toDuration().toMillis();
    }

    public long getDetectorIntervalInSeconds() {
        return getDetectorIntervalInMilliseconds() / 1000;
    }

    public long getDetectorIntervalInMinutes() {
        return getDetectorIntervalInMilliseconds() / 1000 / 60;
    }

    public Duration getDetectionIntervalDuration() {
        return ((IntervalTimeConfiguration) getDetectionInterval()).toDuration();
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public String getDetectorType() {
        return detectorType;
    }

    private void setDetectionDateRange(DetectionDateRange detectionDateRange) {
        this.detectionDateRange = detectionDateRange;
    }

    public DetectionDateRange getDetectionDateRange() {
        return detectionDateRange;
    }

    public boolean isMultientityDetector() {
        return AnomalyDetector.isMultientityDetector(getCategoryField());
    }

    public boolean isMultiCategoryDetector() {
        return categoryFields != null && categoryFields.size() > 1;
    }

    private static boolean isMultientityDetector(List<String> categoryFields) {
        return categoryFields != null && categoryFields.size() > 0;
    }

    // TODO: remove this
    public boolean isRealTimeDetector() {
        return false;
    }

    public boolean invalidShingleSizeRange(Integer shingleSizeToTest) {
        return shingleSizeToTest != null && (shingleSizeToTest < 1 || shingleSizeToTest > AnomalyDetectorSettings.MAX_SHINGLE_SIZE);
    }
}
