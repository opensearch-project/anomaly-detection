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

import java.io.IOException;

import org.opensearch.Version;
import org.opensearch.action.ActionResponse;
import org.opensearch.ad.cluster.ADVersionUtil;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

public class RCFResultResponse extends ActionResponse implements ToXContentObject {
    public static final String RCF_SCORE_JSON_KEY = "rcfScore";
    public static final String CONFIDENCE_JSON_KEY = "confidence";
    public static final String FOREST_SIZE_JSON_KEY = "forestSize";
    public static final String ATTRIBUTION_JSON_KEY = "attribution";
    public static final String TOTAL_UPDATES_JSON_KEY = "total_updates";
    public static final String RELATIVE_INDEX_FIELD_JSON_KEY = "relativeIndex";
    public static final String PAST_VALUES_FIELD_JSON_KEY = "pastValues";
    public static final String EXPECTED_VAL_LIST_FIELD_JSON_KEY = "expectedValuesList";
    public static final String LIKELIHOOD_FIELD_JSON_KEY = "likelihoodOfValues";
    public static final String THRESHOLD_FIELD_JSON_KEY = "threshold";

    private Double rcfScore;
    private Double confidence;
    private Integer forestSize;
    private double[] attribution;
    private Long totalUpdates = 0L;
    private Version remoteAdVersion;
    private Double anomalyGrade;
    private Integer relativeIndex;
    private double[] pastValues;
    private double[][] expectedValuesList;
    private double[] likelihoodOfValues;
    private Double threshold;

    public RCFResultResponse(
        double rcfScore,
        double confidence,
        int forestSize,
        double[] attribution,
        long totalUpdates,
        double grade,
        Version remoteAdVersion,
        Integer relativeIndex,
        double[] pastValues,
        double[][] expectedValuesList,
        double[] likelihoodOfValues,
        Double threshold
    ) {
        this.rcfScore = rcfScore;
        this.confidence = confidence;
        this.forestSize = forestSize;
        this.attribution = attribution;
        this.totalUpdates = totalUpdates;
        this.anomalyGrade = grade;
        this.remoteAdVersion = remoteAdVersion;
        this.relativeIndex = relativeIndex;
        this.pastValues = pastValues;
        this.expectedValuesList = expectedValuesList;
        this.likelihoodOfValues = likelihoodOfValues;
        this.threshold = threshold;
    }

    public RCFResultResponse(StreamInput in) throws IOException {
        super(in);
        this.rcfScore = in.readDouble();
        this.confidence = in.readDouble();
        this.forestSize = in.readVInt();
        this.attribution = in.readDoubleArray();
        if (in.available() > 0) {
            this.totalUpdates = in.readLong();
            this.anomalyGrade = in.readDouble();
            this.relativeIndex = in.readOptionalInt();

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
    }

    public Double getRCFScore() {
        return rcfScore;
    }

    public Double getConfidence() {
        return confidence;
    }

    public Integer getForestSize() {
        return forestSize;
    }

    /**
     * Returns RCF score attribution. Can be null when anomaly grade is less than
     * or equals to 0.
     *
     * @return RCF score attribution.
     */
    public double[] getAttribution() {
        return attribution;
    }

    public Long getTotalUpdates() {
        return totalUpdates;
    }

    public Double getAnomalyGrade() {
        return anomalyGrade;
    }

    public Integer getRelativeIndex() {
        return relativeIndex;
    }

    public double[] getPastValues() {
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
        out.writeDouble(rcfScore);
        out.writeDouble(confidence);
        out.writeVInt(forestSize);
        out.writeDoubleArray(attribution);
        if (ADVersionUtil.compatibleWithVersionOnOrAfter1_1(remoteAdVersion)) {
            out.writeLong(totalUpdates);
            out.writeDouble(anomalyGrade);
            out.writeOptionalInt(relativeIndex);

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
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RCF_SCORE_JSON_KEY, rcfScore);
        builder.field(CONFIDENCE_JSON_KEY, confidence);
        builder.field(FOREST_SIZE_JSON_KEY, forestSize);
        builder.field(ATTRIBUTION_JSON_KEY, attribution);
        builder.field(TOTAL_UPDATES_JSON_KEY, totalUpdates);
        builder.field(CommonName.ANOMALY_GRADE_JSON_KEY, anomalyGrade);
        builder.field(RELATIVE_INDEX_FIELD_JSON_KEY, relativeIndex);
        builder.field(PAST_VALUES_FIELD_JSON_KEY, pastValues);
        builder.field(EXPECTED_VAL_LIST_FIELD_JSON_KEY, expectedValuesList);
        builder.field(LIKELIHOOD_FIELD_JSON_KEY, likelihoodOfValues);
        builder.field(THRESHOLD_FIELD_JSON_KEY, threshold);
        builder.endObject();
        return builder;
    }

}
