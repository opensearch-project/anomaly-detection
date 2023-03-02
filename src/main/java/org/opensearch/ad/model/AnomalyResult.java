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

import static org.opensearch.ad.constant.CommonName.DUMMY_DETECTOR_ID;
import static org.opensearch.ad.constant.CommonName.SCHEMA_VERSION_FIELD;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ad.annotation.Generated;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.util.ParseUtils;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import com.google.common.base.Objects;

/**
 * Include result returned from RCF model and feature data.
 */
public class AnomalyResult implements ToXContentObject, Writeable {
    private static final Logger LOG = LogManager.getLogger(ThresholdingResult.class);
    public static final String PARSE_FIELD_NAME = "AnomalyResult";
    public static final NamedXContentRegistry.Entry XCONTENT_REGISTRY = new NamedXContentRegistry.Entry(
        AnomalyResult.class,
        new ParseField(PARSE_FIELD_NAME),
        it -> parse(it)
    );

    public static final String DETECTOR_ID_FIELD = "detector_id";
    public static final String ANOMALY_SCORE_FIELD = "anomaly_score";
    public static final String ANOMALY_GRADE_FIELD = "anomaly_grade";
    public static final String CONFIDENCE_FIELD = "confidence";
    public static final String FEATURE_DATA_FIELD = "feature_data";
    public static final String DATA_START_TIME_FIELD = "data_start_time";
    public static final String DATA_END_TIME_FIELD = "data_end_time";
    public static final String EXECUTION_START_TIME_FIELD = "execution_start_time";
    public static final String EXECUTION_END_TIME_FIELD = "execution_end_time";
    public static final String ERROR_FIELD = "error";
    public static final String ENTITY_FIELD = "entity";
    public static final String USER_FIELD = "user";
    public static final String TASK_ID_FIELD = "task_id";
    public static final String MODEL_ID_FIELD = "model_id";
    public static final String APPROX_ANOMALY_START_FIELD = "approx_anomaly_start_time";
    public static final String RELEVANT_ATTRIBUTION_FIELD = "relevant_attribution";
    public static final String PAST_VALUES_FIELD = "past_values";
    public static final String EXPECTED_VALUES_FIELD = "expected_values";
    public static final String THRESHOLD_FIELD = "threshold";
    // unused currently. added since odfe 1.4
    public static final String IS_ANOMALY_FIELD = "is_anomaly";

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
    private final Entity entity;
    private User user;
    private final Integer schemaVersion;
    /*
     * model id for easy aggregations of entities. The front end needs to query
     * for entities ordered by the descending order of anomaly grades and the
     * number of anomalies. After supporting multi-category fields, it is hard
     * to write such queries since the entity information is stored in a nested
     * object array. Also, the front end has all code/queries/ helper functions
     * in place to rely on a single key per entity combo. This PR adds model id
     * to anomaly result to help the transition to multi-categorical field less
     * painful.
     */
    private final String modelId;

    /**
     * the approximate time of current anomaly. We might detect anomaly late.  This field
     * is the approximate anomaly time.  I called it approximate because rcf may
     * not receive continuous data. To make it precise, I have to query previous
     * anomaly results and find the what timestamp correspond to a few data points
     * back. Instead, RCF returns the index of anomaly relative to current timestamp.
     * So approAnomalyStartTime is current time + interval * relativeIndex
     * Note {@code relativeIndex <= 0}.  If the shingle size is 4, for example shingle is
     * [0, 0, 1, 0], and this shingle is detected as anomaly, and actually the
     * anomaly is caused by the third item "1", then the relativeIndex will be
     * -1.
     */
    private final Instant approxAnomalyStartTime;

    // a flattened version denoting the basic contribution of each input variable
    private final List<DataByFeatureId> relevantAttribution;

    /*
    pastValues is related to relativeIndex, startOfAnomaly and anomaly grade.
    So if we detect anomaly late, we get the baseDimension values from the past (current is 0).
    That is, we look back relativeIndex * baseDimensions.
    
    For example, current shingle is
    "currentValues": [
    6819.0,
    2375.3333333333335,
    0.0,
    49882.0,
    92070.0,
    5084.0,
    2072.809523809524,
    0.0,
    43529.0,
    91169.0,
    8129.0,
    2582.892857142857,
    12.0,
    54241.0,
    84596.0,
    11174.0,
    3092.9761904761904,
    24.0,
    64952.0,
    78024.0,
    14220.0,
    3603.059523809524,
    37.0,
    75664.0,
    71451.0,
    17265.0,
    4113.142857142857,
    49.0,
    86376.0,
    64878.0,
    16478.0,
    3761.4166666666665,
    37.0,
    78990.0,
    70057.0,
    15691.0,
    3409.690476190476,
    24.0,
    71604.0,
    75236.0
    ],
    Since rcf returns relativeIndex is -2, we look back baseDimension * 2 and get the pastValues:
    "pastValues": [
    17265.0,
    4113.142857142857,
    49.0,
    86376.0,
    64878.0
    ],
    
    So pastValues is null when relativeIndex is 0 or startOfAnomaly is true
    or the current shingle is not an anomaly.
    
    In the UX, if pastValues value is null, we can just show attribution/expected
    value and it is implicit this is due to current input; if pastValues is not
    null, it means the the attribution/expected values are from an old value
    (e.g., 2 steps ago with data [1,2,3]) and we can add a text to explain that.
    */
    private final List<DataByFeatureId> pastValues;

    /*
     * The expected value is only calculated for anomalous detection intervals,
     * and will generate expected value for each feature if detector has multiple
     * features.
     * Currently we expect one set of expected values. In the future, we
     * might give different expected values with differently likelihood. So
     * the two-dimensional array allows us to future-proof our applications.
     * Also, expected values correspond to pastValues if present or current input
     * point otherwise. If pastValues is present, we can add a text on UX to explain
     * we found an anomaly from the past.
     Example:
     "expected_value": [{
        "likelihood": 0.8,
        "value_list": [{
                "feature_id": "blah",
                "value": 1
            },
            {
                "feature_id": "blah2",
                "value": 1
            }
        ]
    }]*/
    private final List<ExpectedValueList> expectedValuesList;

    // rcf score threshold at the time of writing a result
    private final Double threshold;

    // used when indexing exception or error or an empty result
    public AnomalyResult(
        String detectorId,
        String taskId,
        List<FeatureData> featureData,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executionStartTime,
        Instant executionEndTime,
        String error,
        Entity entity,
        User user,
        Integer schemaVersion,
        String modelId
    ) {
        this(
            detectorId,
            taskId,
            Double.NaN,
            Double.NaN,
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
            null
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
        Entity entity,
        User user,
        Integer schemaVersion,
        String modelId,
        Instant approxAnomalyStartTime,
        List<DataByFeatureId> relevantAttribution,
        List<DataByFeatureId> pastValues,
        List<ExpectedValueList> expectedValuesList,
        Double threshold
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
        this.modelId = modelId;
        this.approxAnomalyStartTime = approxAnomalyStartTime;
        this.relevantAttribution = relevantAttribution;
        this.pastValues = pastValues;
        this.expectedValuesList = expectedValuesList;
        this.threshold = threshold;
    }

    /**
     * Factory method that converts raw rcf results to an instance of AnomalyResult
     * @param detectorId Detector Id
     * @param intervalMillis Detector interval
     * @param taskId Task Id
     * @param rcfScore RCF score
     * @param grade anomaly grade
     * @param confidence data confidence
     * @param featureData Feature data
     * @param dataStartTime Data start time
     * @param dataEndTime Data end time
     * @param executionStartTime Execution start time
     * @param executionEndTime Execution end time
     * @param error Error
     * @param entity Entity accessor
     * @param user the user who created a detector
     * @param schemaVersion Result schema version
     * @param modelId Model Id
     * @param relevantAttribution Attribution of the anomaly
     * @param relativeIndex The index of anomaly point relative to current point.
     * @param pastValues The input that caused anomaly if we detector anomaly late
     * @param expectedValuesList Expected values
     * @param likelihoodOfValues Likelihood of the expected values
     * @param threshold Current threshold
     * @return the converted AnomalyResult instance
     */
    public static AnomalyResult fromRawTRCFResult(
        String detectorId,
        long intervalMillis,
        String taskId,
        Double rcfScore,
        Double grade,
        Double confidence,
        List<FeatureData> featureData,
        Instant dataStartTime,
        Instant dataEndTime,
        Instant executionStartTime,
        Instant executionEndTime,
        String error,
        Entity entity,
        User user,
        Integer schemaVersion,
        String modelId,
        double[] relevantAttribution,
        Integer relativeIndex,
        double[] pastValues,
        double[][] expectedValuesList,
        double[] likelihoodOfValues,
        Double threshold
    ) {
        List<DataByFeatureId> convertedRelevantAttribution = null;
        List<DataByFeatureId> convertedPastValuesList = null;
        List<ExpectedValueList> convertedExpectedValues = null;

        if (grade > 0) {
            int featureSize = featureData.size();
            if (relevantAttribution != null) {
                if (relevantAttribution.length == featureSize) {
                    convertedRelevantAttribution = new ArrayList<>(featureSize);
                    for (int j = 0; j < featureSize; j++) {
                        convertedRelevantAttribution.add(new DataByFeatureId(featureData.get(j).getFeatureId(), relevantAttribution[j]));
                    }
                } else {
                    LOG
                        .error(
                            new ParameterizedMessage(
                                "Attribution array size does not match.  Expected [{}] but got [{}]",
                                featureSize,
                                relevantAttribution.length
                            )
                        );
                }
            }

            if (pastValues != null) {
                if (pastValues.length == featureSize) {
                    convertedPastValuesList = new ArrayList<>(featureSize);
                    for (int j = 0; j < featureSize; j++) {
                        convertedPastValuesList.add(new DataByFeatureId(featureData.get(j).getFeatureId(), pastValues[j]));
                    }
                } else {
                    LOG
                        .error(
                            new ParameterizedMessage(
                                "Past value array size does not match.  Expected [{}] but got [{}]",
                                featureSize,
                                pastValues.length
                            )
                        );
                }
            }

            if (expectedValuesList != null && expectedValuesList.length > 0) {
                int numberOfExpectedLists = expectedValuesList.length;
                int numberOfExpectedVals = expectedValuesList[0].length;
                if (numberOfExpectedVals == featureSize && likelihoodOfValues.length == numberOfExpectedLists) {
                    convertedExpectedValues = new ArrayList<>(numberOfExpectedLists);
                    for (int j = 0; j < numberOfExpectedLists; j++) {
                        List<DataByFeatureId> valueList = new ArrayList<>(featureSize);
                        for (int k = 0; k < featureSize; k++) {
                            valueList.add(new DataByFeatureId(featureData.get(k).getFeatureId(), expectedValuesList[j][k]));
                        }
                        convertedExpectedValues.add(new ExpectedValueList(likelihoodOfValues[j], valueList));
                    }
                } else if (numberOfExpectedVals != featureSize) {
                    LOG
                        .error(
                            new ParameterizedMessage(
                                "expected value array mismatch.  Expected [{}] actual [{}].",
                                featureSize,
                                numberOfExpectedVals
                            )
                        );
                } else {
                    LOG
                        .error(
                            new ParameterizedMessage(
                                "likelihood and expected array mismatch: Likelihood [{}] expected value [{}].",
                                likelihoodOfValues.length,
                                numberOfExpectedLists
                            )
                        );
                }
            }
        }

        return new AnomalyResult(
            detectorId,
            taskId,
            rcfScore,
            grade,
            confidence,
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
            (relativeIndex == null || dataStartTime == null)
                ? null
                : Instant.ofEpochMilli(dataStartTime.toEpochMilli() + relativeIndex * intervalMillis),
            convertedRelevantAttribution,
            convertedPastValuesList,
            convertedExpectedValues,
            threshold
        );
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
            this.entity = new Entity(input);
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
        this.modelId = input.readOptionalString();

        // if anomaly is caused by current input, we don't show approximate time
        this.approxAnomalyStartTime = input.readOptionalInstant();

        int attributeNumber = input.readVInt();
        if (attributeNumber <= 0) {
            this.relevantAttribution = null;
        } else {
            this.relevantAttribution = new ArrayList<>(attributeNumber);
            for (int i = 0; i < attributeNumber; i++) {
                relevantAttribution.add(new DataByFeatureId(input));
            }
        }

        int pastValueNumber = input.readVInt();
        if (pastValueNumber <= 0) {
            this.pastValues = null;
        } else {
            this.pastValues = new ArrayList<>(pastValueNumber);
            for (int i = 0; i < pastValueNumber; i++) {
                pastValues.add(new DataByFeatureId(input));
            }
        }

        int expectedValuesNumber = input.readVInt();
        if (expectedValuesNumber <= 0) {
            this.expectedValuesList = null;
        } else {
            this.expectedValuesList = new ArrayList<>();
            for (int i = 0; i < expectedValuesNumber; i++) {
                expectedValuesList.add(new ExpectedValueList(input));
            }
        }

        this.threshold = input.readOptionalDouble();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder
            .startObject()
            .field(DETECTOR_ID_FIELD, detectorId)
            .field(SCHEMA_VERSION_FIELD, schemaVersion);
        // In normal AD result, we always pass data start/end times. In custom result index,
        // we need to write/delete a dummy AD result to verify if user has write permission
        // to the custom result index. Just pass in null start/end time for this dummy anomaly
        // result to make sure it won't be queried by mistake.
        if (dataStartTime != null) {
            xContentBuilder.field(DATA_START_TIME_FIELD, dataStartTime.toEpochMilli());
        }
        if (dataEndTime != null) {
            xContentBuilder.field(DATA_END_TIME_FIELD, dataEndTime.toEpochMilli());
        }
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
            xContentBuilder.field(ENTITY_FIELD, entity);
        }
        if (user != null) {
            xContentBuilder.field(USER_FIELD, user);
        }
        if (taskId != null) {
            xContentBuilder.field(TASK_ID_FIELD, taskId);
        }
        if (modelId != null) {
            xContentBuilder.field(MODEL_ID_FIELD, modelId);
        }

        // output extra fields such as attribution and expected only when this is an anomaly
        if (anomalyGrade != null && anomalyGrade > 0) {
            if (approxAnomalyStartTime != null) {
                xContentBuilder.field(APPROX_ANOMALY_START_FIELD, approxAnomalyStartTime.toEpochMilli());
            }
            if (relevantAttribution != null) {
                xContentBuilder.array(RELEVANT_ATTRIBUTION_FIELD, relevantAttribution.toArray());
            }
            if (pastValues != null) {
                xContentBuilder.array(PAST_VALUES_FIELD, pastValues.toArray());
            }

            if (expectedValuesList != null) {
                xContentBuilder.array(EXPECTED_VALUES_FIELD, expectedValuesList.toArray());
            }
        }

        if (threshold != null && !threshold.isNaN()) {
            xContentBuilder.field(THRESHOLD_FIELD, threshold);
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
        Entity entity = null;
        User user = null;
        Integer schemaVersion = CommonValue.NO_SCHEMA_VERSION;
        String taskId = null;
        String modelId = null;
        Instant approAnomalyStartTime = null;
        List<DataByFeatureId> relavantAttribution = new ArrayList<>();
        List<DataByFeatureId> pastValues = new ArrayList<>();
        List<ExpectedValueList> expectedValues = new ArrayList<>();
        Double threshold = null;

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
                    entity = Entity.parse(parser);
                    break;
                case USER_FIELD:
                    user = User.parse(parser);
                    break;
                case SCHEMA_VERSION_FIELD:
                    schemaVersion = parser.intValue();
                    break;
                case TASK_ID_FIELD:
                    taskId = parser.text();
                    break;
                case MODEL_ID_FIELD:
                    modelId = parser.text();
                    break;
                case APPROX_ANOMALY_START_FIELD:
                    approAnomalyStartTime = ParseUtils.toInstant(parser);
                    break;
                case RELEVANT_ATTRIBUTION_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        relavantAttribution.add(DataByFeatureId.parse(parser));
                    }
                    break;
                case PAST_VALUES_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        pastValues.add(DataByFeatureId.parse(parser));
                    }
                    break;
                case EXPECTED_VALUES_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        expectedValues.add(ExpectedValueList.parse(parser));
                    }
                    break;
                case THRESHOLD_FIELD:
                    threshold = parser.doubleValue();
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
            entity,
            user,
            schemaVersion,
            modelId,
            approAnomalyStartTime,
            relavantAttribution,
            pastValues,
            expectedValues,
            threshold
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
        return Objects.equal(detectorId, that.detectorId)
            && Objects.equal(taskId, that.taskId)
            && Objects.equal(anomalyScore, that.anomalyScore)
            && Objects.equal(anomalyGrade, that.anomalyGrade)
            && Objects.equal(confidence, that.confidence)
            && Objects.equal(featureData, that.featureData)
            && Objects.equal(dataStartTime, that.dataStartTime)
            && Objects.equal(dataEndTime, that.dataEndTime)
            && Objects.equal(executionStartTime, that.executionStartTime)
            && Objects.equal(executionEndTime, that.executionEndTime)
            && Objects.equal(error, that.error)
            && Objects.equal(entity, that.entity)
            && Objects.equal(modelId, that.modelId)
            && Objects.equal(approxAnomalyStartTime, that.approxAnomalyStartTime)
            && Objects.equal(relevantAttribution, that.relevantAttribution)
            && Objects.equal(pastValues, that.pastValues)
            && Objects.equal(expectedValuesList, that.expectedValuesList)
            && Objects.equal(threshold, that.threshold);
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects
            .hashCode(
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
                entity,
                modelId,
                approxAnomalyStartTime,
                relevantAttribution,
                pastValues,
                expectedValuesList,
                threshold
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
            .append("modelId", modelId)
            .append("approAnomalyStartTime", approxAnomalyStartTime)
            .append("relavantAttribution", relevantAttribution)
            .append("pastValues", pastValues)
            .append("expectedValuesList", StringUtils.join(expectedValuesList, "|"))
            .append("threshold", threshold)
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

    public Entity getEntity() {
        return entity;
    }

    public String getModelId() {
        return modelId;
    }

    public Instant getApproAnomalyStartTime() {
        return approxAnomalyStartTime;
    }

    public List<DataByFeatureId> getRelavantAttribution() {
        return relevantAttribution;
    }

    public List<DataByFeatureId> getPastValues() {
        return pastValues;
    }

    public List<ExpectedValueList> getExpectedValuesList() {
        return expectedValuesList;
    }

    public Double getThreshold() {
        return threshold;
    }

    /**
     * Anomaly result index consists of overwhelmingly (99.5%) zero-grade non-error documents.
     * This function exclude the majority case.
     * @return whether the anomaly result is important when the anomaly grade is not 0
     * or error is there.
     */
    public boolean isHighPriority() {
        // AnomalyResult.toXContent won't record Double.NaN and thus make it null
        return (getAnomalyGrade() != null && getAnomalyGrade() > 0) || getError() != null;
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
            entity.writeTo(out);
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
        out.writeOptionalString(modelId);

        out.writeOptionalInstant(approxAnomalyStartTime);

        if (relevantAttribution != null) {
            out.writeVInt(relevantAttribution.size());
            for (DataByFeatureId attribution : relevantAttribution) {
                attribution.writeTo(out);
            }
        } else {
            out.writeVInt(0);
        }

        if (pastValues != null) {
            out.writeVInt(pastValues.size());
            for (DataByFeatureId value : pastValues) {
                value.writeTo(out);
            }
        } else {
            out.writeVInt(0);
        }

        if (expectedValuesList != null) {
            out.writeVInt(expectedValuesList.size());
            for (ExpectedValueList value : expectedValuesList) {
                value.writeTo(out);
            }
        } else {
            out.writeVInt(0);
        }

        out.writeOptionalDouble(threshold);
    }

    public static AnomalyResult getDummyResult() {
        return new AnomalyResult(
            DUMMY_DETECTOR_ID,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            CommonValue.NO_SCHEMA_VERSION,
            null
        );
    }
}
