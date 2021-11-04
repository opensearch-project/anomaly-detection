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

package org.opensearch.ad.transport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.opensearch.action.ActionResponse;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.FeatureData;
import org.opensearch.common.io.stream.InputStreamStreamInput;
import org.opensearch.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.commons.authuser.User;

public class AnomalyResultResponse extends ActionResponse implements ToXContentObject {
    public static final String ANOMALY_GRADE_JSON_KEY = "anomalyGrade";
    public static final String CONFIDENCE_JSON_KEY = "confidence";
    public static final String ANOMALY_SCORE_JSON_KEY = "anomalyScore";
    public static final String ERROR_JSON_KEY = "error";
    public static final String FEATURES_JSON_KEY = "features";
    public static final String FEATURE_VALUE_JSON_KEY = "value";
    public static final String RCF_TOTAL_UPDATES_JSON_KEY = "rcfTotalUpdates";
    public static final String DETECTOR_INTERVAL_IN_MINUTES_JSON_KEY = "detectorIntervalInMinutes";
    public static final String RELATIVE_INDEX_FIELD_JSON_KEY = "relativeIndex";
    public static final String RELEVANT_ATTRIBUTION_FIELD_JSON_KEY = "relevantAttribution";
    public static final String PAST_VALUES_FIELD_JSON_KEY = "pastValues";
    public static final String EXPECTED_VAL_LIST_FIELD_JSON_KEY = "expectedValuesList";
    public static final String LIKELIHOOD_FIELD_JSON_KEY = "likelihoodOfValues";
    public static final String THRESHOLD_FIELD_JSON_KEY = "threshold";

    private Double anomalyGrade;
    private Double confidence;
    private Double anomalyScore;
    private String error;
    private List<FeatureData> features;
    private Long rcfTotalUpdates;
    private Long detectorIntervalInMinutes;
    private Boolean isHCDetector;
    private Integer relativeIndex;
    private double[] relevantAttribution;
    private double[] pastValues;
    private double[][] expectedValuesList;
    private double[] likelihoodOfValues;
    private Double threshold;

    // used when returning an error/exception or empty result
    public AnomalyResultResponse(List<FeatureData> features, String error, Long rcfTotalUpdates, Long detectorIntervalInMinutes) {
        this(
            Double.NaN,
            Double.NaN,
            Double.NaN,
            features,
            error,
            rcfTotalUpdates,
            detectorIntervalInMinutes,
            null,
            null,
            null,
            null,
            null,
            null,
            Double.NaN
        );
    }

    public AnomalyResultResponse(
        Double anomalyGrade,
        Double confidence,
        Double anomalyScore,
        List<FeatureData> features,
        String error,
        Long rcfTotalUpdates,
        Long detectorIntervalInMinutes,
        Boolean isHCDetector,
        Integer relativeIndex,
        double[] currentTimeAttribution,
        double[] pastValues,
        double[][] expectedValuesList,
        double[] likelihoodOfValues,
        Double threshold
    ) {
        this.anomalyGrade = anomalyGrade;
        this.confidence = confidence;
        this.anomalyScore = anomalyScore;
        this.features = features;
        this.error = error;
        this.rcfTotalUpdates = rcfTotalUpdates;
        this.detectorIntervalInMinutes = detectorIntervalInMinutes;
        this.isHCDetector = isHCDetector;
        this.relativeIndex = relativeIndex;
        this.relevantAttribution = currentTimeAttribution;
        this.pastValues = pastValues;
        this.expectedValuesList = expectedValuesList;
        this.likelihoodOfValues = likelihoodOfValues;
        this.threshold = threshold;
    }

    public AnomalyResultResponse(StreamInput in) throws IOException {
        super(in);
        anomalyGrade = in.readDouble();
        confidence = in.readDouble();
        anomalyScore = in.readDouble();
        int size = in.readVInt();
        features = new ArrayList<FeatureData>();
        for (int i = 0; i < size; i++) {
            features.add(new FeatureData(in));
        }
        error = in.readOptionalString();
        // new field added since AD 1.1
        // Only send AnomalyResultRequest to local node, no need to change this part for BWC
        rcfTotalUpdates = in.readOptionalLong();
        detectorIntervalInMinutes = in.readOptionalLong();
        isHCDetector = in.readOptionalBoolean();

        this.relativeIndex = in.readOptionalInt();

        // input.readOptionalArray(i -> i.readDouble(), double[]::new) results in
        // compiler error as readOptionalArray does not work for primitive array.
        // use readDoubleArray and readBoolean instead
        if (in.readBoolean()) {
            this.relevantAttribution = in.readDoubleArray();
        } else {
            this.relevantAttribution = null;
        }

        if (in.readBoolean()) {
            this.pastValues = in.readDoubleArray();
        } else {
            this.pastValues = null;
        }

        if (in.readBoolean()) {
            int numberofExpectedVals = in.readVInt();
            this.expectedValuesList = new double[numberofExpectedVals][];
            for (int i = 0; i < numberofExpectedVals; i++) {
                expectedValuesList[i] = in.readDoubleArray();
            }
        } else {
            this.expectedValuesList = null;
        }

        if (in.readBoolean()) {
            this.likelihoodOfValues = in.readDoubleArray();
        } else {
            this.likelihoodOfValues = null;
        }

        this.threshold = in.readOptionalDouble();
    }

    public double getAnomalyGrade() {
        return anomalyGrade;
    }

    public List<FeatureData> getFeatures() {
        return features;
    }

    public double getConfidence() {
        return confidence;
    }

    public double getAnomalyScore() {
        return anomalyScore;
    }

    public String getError() {
        return error;
    }

    public Long getRcfTotalUpdates() {
        return rcfTotalUpdates;
    }

    public Long getDetectorIntervalInMinutes() {
        return detectorIntervalInMinutes;
    }

    public Boolean isHCDetector() {
        return isHCDetector;
    }

    public Integer getRelativeIndex() {
        return relativeIndex;
    }

    public double[] getCurrentTimeAttribution() {
        return relevantAttribution;
    }

    public double[] getOldValues() {
        return pastValues;
    }

    public double[][] getExpectedValuesList() {
        return expectedValuesList;
    }

    public double[] getLikelihoodOfValues() {
        return likelihoodOfValues;
    }

    public Double getThreshold() {
        return threshold;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(anomalyGrade);
        out.writeDouble(confidence);
        out.writeDouble(anomalyScore);
        out.writeVInt(features.size());
        for (FeatureData feature : features) {
            feature.writeTo(out);
        }
        out.writeOptionalString(error);
        out.writeOptionalLong(rcfTotalUpdates);
        out.writeOptionalLong(detectorIntervalInMinutes);
        out.writeOptionalBoolean(isHCDetector);

        out.writeOptionalInt(relativeIndex);

        // writeOptionalArray does not work for primitive array. Use WriteDoubleArray
        // instead.
        if (relevantAttribution != null) {
            out.writeBoolean(true);
            out.writeDoubleArray(relevantAttribution);
        } else {
            out.writeBoolean(false);
        }

        if (pastValues != null) {
            out.writeBoolean(true);
            out.writeDoubleArray(pastValues);
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

        if (likelihoodOfValues != null) {
            out.writeBoolean(true);
            out.writeDoubleArray(likelihoodOfValues);
        } else {
            out.writeBoolean(false);
        }

        out.writeOptionalDouble(threshold);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ANOMALY_GRADE_JSON_KEY, anomalyGrade);
        builder.field(CONFIDENCE_JSON_KEY, confidence);
        builder.field(ANOMALY_SCORE_JSON_KEY, anomalyScore);
        builder.field(ERROR_JSON_KEY, error);
        builder.startArray(FEATURES_JSON_KEY);
        for (FeatureData feature : features) {
            feature.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(RCF_TOTAL_UPDATES_JSON_KEY, rcfTotalUpdates);
        builder.field(DETECTOR_INTERVAL_IN_MINUTES_JSON_KEY, detectorIntervalInMinutes);
        builder.field(RELATIVE_INDEX_FIELD_JSON_KEY, relativeIndex);
        builder.field(RELEVANT_ATTRIBUTION_FIELD_JSON_KEY, relevantAttribution);
        builder.field(PAST_VALUES_FIELD_JSON_KEY, pastValues);
        builder.field(EXPECTED_VAL_LIST_FIELD_JSON_KEY, expectedValuesList);
        builder.field(LIKELIHOOD_FIELD_JSON_KEY, likelihoodOfValues);
        builder.field(THRESHOLD_FIELD_JSON_KEY, threshold);
        builder.endObject();
        return builder;
    }

    public static AnomalyResultResponse fromActionResponse(final ActionResponse actionResponse) {
        if (actionResponse instanceof AnomalyResultResponse) {
            return (AnomalyResultResponse) actionResponse;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionResponse.writeTo(osso);
            try (InputStreamStreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new AnomalyResultResponse(input);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse ActionResponse into AnomalyResultResponse", e);
        }
    }

    /**
    *
    * Convert AnomalyResultResponse to AnomalyResult
    *
    * @param detectorId Detector Id
    * @param dataStartInstant data start time
    * @param dataEndInstant data end time
    * @param executionStartInstant  execution start time
    * @param executionEndInstant execution end time
    * @param schemaVersion Schema version
    * @param user Detector author
    * @param error Error
    * @return converted AnomalyResult
    */
    public AnomalyResult toAnomalyResult(
        String detectorId,
        Instant dataStartInstant,
        Instant dataEndInstant,
        Instant executionStartInstant,
        Instant executionEndInstant,
        Integer schemaVersion,
        User user,
        String error
    ) {
        // Detector interval in milliseconds
        long detectorIntervalMilli = Duration.between(dataStartInstant, dataEndInstant).toMillis();
        return AnomalyResult
            .fromRawTRCFResult(
                detectorId,
                detectorIntervalMilli,
                null, // real time results have no task id
                anomalyScore,
                anomalyGrade,
                confidence,
                features,
                dataStartInstant,
                dataEndInstant,
                executionStartInstant,
                executionEndInstant,
                error,
                null,
                user,
                schemaVersion,
                null, // single-stream real-time has no model id
                relevantAttribution,
                relativeIndex,
                pastValues,
                expectedValuesList,
                likelihoodOfValues,
                threshold
            );
    }
}
