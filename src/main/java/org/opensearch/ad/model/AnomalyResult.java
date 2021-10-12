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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.ad.annotation.Generated;
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
import com.google.common.collect.ImmutableMap;

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
    // TODO: use the file anomaly-results.json as the single source of truth for AD result index mapping.
    // Support upgrading custom result index.
    public static final ImmutableMap<String, String> AD_RESULT_FIELD_CONFIGS = ImmutableMap
        .<String, String>builder()
        .put(ANOMALY_GRADE_FIELD, "{type=double}")
        .put(ANOMALY_SCORE_FIELD, "{type=double}")
        .put(CONFIDENCE_FIELD, "{type=double}")
        .put(DATA_START_TIME_FIELD, "{type=date, format=strict_date_time||epoch_millis}")
        .put(DATA_END_TIME_FIELD, "{type=date, format=strict_date_time||epoch_millis}")
        .put(DETECTOR_ID_FIELD, "{type=keyword}")
        .put(EXECUTION_START_TIME_FIELD, "{type=date, format=strict_date_time||epoch_millis}")
        .put(FEATURE_DATA_FIELD, "{type=nested, properties={data={type=double}, feature_id={type=keyword}}}")
        .put(MODEL_ID_FIELD, "{type=keyword}")
        .put(TASK_ID_FIELD, "{type=keyword}")
        .put(
            USER_FIELD,
            "{type=nested, properties={backend_roles={type=text, fields={keyword={type=keyword}}}, custom_attribute_names={type=text,"
                + " fields={keyword={type=keyword}}}, name={type=text, fields={keyword={type=keyword, ignore_above=256}}},"
                + " roles={type=text, fields={keyword={type=keyword}}}}}"
        )
        .build();
    public static final String TOTAL_UPDATES_FIELD = "totalUpdates";
    public static final String START_OF_ANOMALY_FIELD = "startOfAnomaly";
    public static final String IN_HIGH_SCORE_REGION_FIELD = "inHighScoreRegion";
    public static final String RELATIVE_INDEX_FIELD = "relativeIndex";
    public static final String CURRENT_TIME_ATTRIBUTION_FIELD = "currentTimeAttribution";
    public static final String OLD_VALUES_FIELD = "oldValues";
    public static final String EXPECTED_VAL_LIST_FIELD = "expectedValuesList";
    public static final String THRESHOLD_FIELD = "threshold";

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

    // sequence index (the number of updates to RCF) -- it is possible in imputation
    // that
    // the number of updates more than the input tuples seen by the overall program
    private final Long totalUpdates;

    // flag indicating if the anomaly is the start of an anomaly or part of a run of
    // anomalies
    private final Boolean startOfAnomaly;

    // flag indicating if the time stamp is in elevated score region to be
    // considered as anomaly
    private final Boolean inHighScoreRegion;

    /**
     * position of the anomaly vis a vis the current time (can be -ve) if anomaly is
     * detected late, which can and should happen sometime; for shingle size 1; this
     * is always 0
     */
    private final Integer relativeIndex;

    // a flattened version denoting the basic contribution of each input variable
    private final double[] currentTimeAttribution;

    /*
    oldValues is related to relativeIndex and startOfAnomaly.
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
    Since relativeIndex is -2, we look back baseDimension * 2 and get the oldValues:
    "startOfAnomaly": true,
    "inHighScoreRegion": true,
    "relativeIndex": -2,
    "oldValues": [
    17265.0,
    4113.142857142857,
    49.0,
    86376.0,
    64878.0
    ],

    So oldValues is null when relativeIndex is 0 or startOfAnomaly is true.*/
    private final double[] oldValues;

    // expected values, currently set to maximum 1 expected. In the future, we
    // might give different expected values with differently likelihood. So
    // the two-dimensional array allows us to future-proof our applications.
    // Also, expected values correspond to oldValues if present or current input
    // point otherwise. If oldValues is present, it will take effort to show this
    // on UX since we found an anomaly from the past (old values).
    private final double[][] expectedValuesList;

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
        Long totalUpdates,
        Boolean startOfAnomaly,
        Boolean inHighScoreRegion,
        Integer relativeIndex,
        double[] currentTimeAttribution,
        double[] oldValues,
        double[][] expectedValuesList,
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
        this.totalUpdates = totalUpdates;
        this.startOfAnomaly = startOfAnomaly;
        this.inHighScoreRegion = inHighScoreRegion;
        this.relativeIndex = relativeIndex;
        this.currentTimeAttribution = currentTimeAttribution;
        this.oldValues = oldValues;
        this.expectedValuesList = expectedValuesList;
        this.threshold = threshold;
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

        this.totalUpdates = input.readOptionalLong();
        this.startOfAnomaly = input.readOptionalBoolean();
        this.inHighScoreRegion = input.readOptionalBoolean();
        this.relativeIndex = input.readOptionalInt();

        // input.readOptionalArray(i -> i.readDouble(), double[]::new) results in
        // compiler error as readOptionalArray does not work for primitive array.
        // use readDoubleArray and readBoolean instead
        if (input.readBoolean()) {
            this.currentTimeAttribution = input.readDoubleArray();
        } else {
            this.currentTimeAttribution = null;
        }

        if (input.readBoolean()) {
            this.oldValues = input.readDoubleArray();
        } else {
            this.oldValues = null;
        }

        if (input.readBoolean()) {
            int numberofExpectedVals = input.readVInt();
            this.expectedValuesList = new double[numberofExpectedVals][];
            for (int i = 0; i < numberofExpectedVals; i++) {
                expectedValuesList[i] = input.readDoubleArray();
            }
        } else {
            this.expectedValuesList = null;
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
            if (totalUpdates != null) {
                xContentBuilder.field(TOTAL_UPDATES_FIELD, totalUpdates);
            }
            if (startOfAnomaly != null) {
                xContentBuilder.field(START_OF_ANOMALY_FIELD, startOfAnomaly);
            }
            if (inHighScoreRegion != null) {
                xContentBuilder.field(IN_HIGH_SCORE_REGION_FIELD, inHighScoreRegion);
            }
            if (relativeIndex != null) {
                xContentBuilder.field(RELATIVE_INDEX_FIELD, relativeIndex);
            }
            if (currentTimeAttribution != null) {
                xContentBuilder.array(CURRENT_TIME_ATTRIBUTION_FIELD, currentTimeAttribution);
            }
            if (oldValues != null) {
                xContentBuilder.array(OLD_VALUES_FIELD, oldValues);
            }
            // In OpenSearch, an array of arrays like [ 1, [ 2, 3 ]] is the equivalent of [ 1, 2, 3 ]
            // Also, even though expectedValuesList if a two-dimensional array, it has only one nested
            // array inside in current implementation.
            // Index the field by flattening the two-dimensional array expectedValuesList to
            // a one-dimensional array.
            if (expectedValuesList != null) {
                // int[2][] { int[5] { 1, 2, 6, 7, 2 }, int[5] {2,44,55,2, 3}}
                // will be flattened to
                // int[10] { 1, 2, 6, 7, 2, 2, 44, 55, 2, 3}
                double[] flattenedExpectedVals = Arrays.stream(expectedValuesList).flatMapToDouble(Arrays::stream).toArray();
                xContentBuilder.array(EXPECTED_VAL_LIST_FIELD, flattenedExpectedVals);
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
        Long totalUpdates = null;
        Boolean startOfAnomaly = false;
        Boolean inHighScoreRegion = false;
        Integer relativeIndex = 0;
        double[] currentTimeAttribution = null;
        double[] oldValues = null;
        double[] flattenedExpectedValues = null;
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
                case TOTAL_UPDATES_FIELD:
                    totalUpdates = parser.longValue();
                    break;
                case START_OF_ANOMALY_FIELD:
                    startOfAnomaly = parser.booleanValue();
                    break;
                case IN_HIGH_SCORE_REGION_FIELD:
                    inHighScoreRegion = parser.booleanValue();
                    break;
                case RELATIVE_INDEX_FIELD:
                    relativeIndex = parser.intValue();
                    break;
                case CURRENT_TIME_ATTRIBUTION_FIELD:
                    currentTimeAttribution = ParseUtils.parseDoubleArray(parser);
                    break;
                case OLD_VALUES_FIELD:
                    oldValues = ParseUtils.parseDoubleArray(parser);
                    break;
                case EXPECTED_VAL_LIST_FIELD:
                    flattenedExpectedValues = ParseUtils.parseDoubleArray(parser);
                    break;
                case THRESHOLD_FIELD:
                    threshold = parser.doubleValue();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }

        // unflatten flattenedExpectedValues
        double[][] expectedValuesList = null;
        if (currentTimeAttribution != null && flattenedExpectedValues != null) {
            int dimension = currentTimeAttribution.length;
            int numberOfExpectedVals = flattenedExpectedValues.length / dimension;
            if (numberOfExpectedVals > 0) {
                expectedValuesList = new double[numberOfExpectedVals][dimension];
                if (numberOfExpectedVals == 1) {
                    expectedValuesList[0] = flattenedExpectedValues;
                } else {
                    for (int i = 0; i < numberOfExpectedVals; i++) {
                        int start = i * dimension;
                        int end = start + dimension;
                        expectedValuesList[i] = Arrays.copyOfRange(flattenedExpectedValues, start, end);
                    }
                }

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
            totalUpdates,
            startOfAnomaly,
            inHighScoreRegion,
            relativeIndex,
            currentTimeAttribution,
            oldValues,
            expectedValuesList,
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
            && Objects.equal(totalUpdates, that.totalUpdates)
            && Objects.equal(startOfAnomaly, that.startOfAnomaly)
            && Objects.equal(inHighScoreRegion, that.inHighScoreRegion)
            && Objects.equal(relativeIndex, that.relativeIndex)
            && Arrays.equals(currentTimeAttribution, currentTimeAttribution)
            && Arrays.equals(oldValues, oldValues)
            && Arrays.deepEquals(expectedValuesList, expectedValuesList)
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
                totalUpdates,
                startOfAnomaly,
                inHighScoreRegion,
                relativeIndex,
                Arrays.hashCode(currentTimeAttribution),
                Arrays.hashCode(oldValues),
                Arrays.deepHashCode(expectedValuesList),
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
            .append("totalUpdates", totalUpdates)
            .append("startOfAnomaly", startOfAnomaly)
            .append("inHighScoreRegion", inHighScoreRegion)
            .append("relativeIndex", relativeIndex)
            .append("currentTimeAttribution", Arrays.toString(currentTimeAttribution))
            .append("oldValues", Arrays.toString(oldValues))
            .append("expectedValuesList", Arrays.deepToString(expectedValuesList))
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

    public Long getTotalUpdates() {
        return totalUpdates;
    }

    public Boolean isStartOfAnomaly() {
        return startOfAnomaly;
    }

    public Boolean isInHighScoreRegion() {
        return inHighScoreRegion;
    }

    public Integer getRelativeIndex() {
        return relativeIndex;
    }

    public double[] getCurrentTimeAttribution() {
        return currentTimeAttribution;
    }

    public double[] getOldValues() {
        return oldValues;
    }

    public double[][] getExpectedValuesList() {
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

        out.writeOptionalLong(totalUpdates);
        out.writeOptionalBoolean(startOfAnomaly);
        out.writeOptionalBoolean(inHighScoreRegion);
        out.writeOptionalInt(relativeIndex);

        // writeOptionalArray does not work for primitive array. Use WriteDoubleArray
        // instead.
        if (currentTimeAttribution != null) {
            out.writeBoolean(true);
            out.writeDoubleArray(currentTimeAttribution);
        } else {
            out.writeBoolean(false);
        }

        if (oldValues != null) {
            out.writeBoolean(true);
            out.writeDoubleArray(oldValues);
        } else {
            out.writeBoolean(false);
        }

        if (expectedValuesList != null) {
            out.writeBoolean(true);
            int numberofExpectedVals = expectedValuesList.length;
            out.writeVInt(expectedValuesList.length);
            for (int i = 0; i < numberofExpectedVals; i++) {
                out.writeDoubleArray(expectedValuesList[i]);
            }
        } else {
            out.writeBoolean(false);
        }

        out.writeOptionalDouble(threshold);
    }

    public static AnomalyResult getDummyResult() {
        return new AnomalyResult(
            DUMMY_DETECTOR_ID,
            Double.NaN,
            Double.NaN,
            Double.NaN,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            CommonValue.NO_SCHEMA_VERSION
        );
    }
}
