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
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class RCFResultResponse extends ActionResponse implements ToXContentObject {
    public static final String RCF_SCORE_JSON_KEY = "rcfScore";
    public static final String CONFIDENCE_JSON_KEY = "confidence";
    public static final String FOREST_SIZE_JSON_KEY = "forestSize";
    public static final String ATTRIBUTION_JSON_KEY = "attribution";
    public static final String TOTAL_UPDATES_JSON_KEY = "total_updates";
    public static final String START_OF_ANOMALY_FIELD_JSON_KEY = "startOfAnomaly";
    public static final String IN_HIGH_SCORE_REGION_FIELD_JSON_KEY = "inHighScoreRegion";
    public static final String RELATIVE_INDEX_FIELD_JSON_KEY = "relativeIndex";
    public static final String OLD_VALUES_FIELD_JSON_KEY = "oldValues";
    public static final String EXPECTED_VAL_LIST_FIELD_JSON_KEY = "expectedValuesList";
    public static final String THRESHOLD_FIELD_JSON_KEY = "threshold";

    private Double rcfScore;
    private Double confidence;
    private Integer forestSize;
    private double[] attribution;
    private Long totalUpdates = 0L;
    private Version remoteAdVersion;
    private Double anomalyGrade;
    private Boolean startOfAnomaly;
    private Boolean inHighScoreRegion;
    private Integer relativeIndex;
    private double[] oldValues;
    private double[][] expectedValuesList;
    private Double threshold;

    public RCFResultResponse(
        double rcfScore,
        double confidence,
        int forestSize,
        double[] attribution,
        long totalUpdates,
        double grade,
        Version remoteAdVersion,
        Boolean startOfAnomaly,
        Boolean inHighScoreRegion,
        Integer relativeIndex,
        double[] oldValues,
        double[][] expectedValuesList,
        Double threshold
    ) {
        this.rcfScore = rcfScore;
        this.confidence = confidence;
        this.forestSize = forestSize;
        this.attribution = attribution;
        this.totalUpdates = totalUpdates;
        this.anomalyGrade = grade;
        this.remoteAdVersion = remoteAdVersion;
        this.startOfAnomaly = startOfAnomaly;
        this.inHighScoreRegion = inHighScoreRegion;
        this.relativeIndex = relativeIndex;
        this.oldValues = oldValues;
        this.expectedValuesList = expectedValuesList;
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
            this.startOfAnomaly = in.readOptionalBoolean();
            this.inHighScoreRegion = in.readOptionalBoolean();
            this.relativeIndex = in.readOptionalInt();

            if (in.readBoolean()) {
                this.oldValues = in.readDoubleArray();
            } else {
                this.oldValues = null;
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

    public Boolean getStartOfAnomaly() {
        return startOfAnomaly;
    }

    public Boolean getInHighScoreRegion() {
        return inHighScoreRegion;
    }

    public Integer getRelativeIndex() {
        return relativeIndex;
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(rcfScore);
        out.writeDouble(confidence);
        out.writeVInt(forestSize);
        out.writeDoubleArray(attribution);
        if (ADVersionUtil.compatibleWithVersionOnOrAfter1_1(remoteAdVersion)) {
            out.writeLong(totalUpdates);
            out.writeDouble(anomalyGrade);
            out.writeOptionalBoolean(startOfAnomaly);
            out.writeOptionalBoolean(inHighScoreRegion);
            out.writeOptionalInt(relativeIndex);

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
        builder.field(START_OF_ANOMALY_FIELD_JSON_KEY, startOfAnomaly);
        builder.field(IN_HIGH_SCORE_REGION_FIELD_JSON_KEY, inHighScoreRegion);
        builder.field(RELATIVE_INDEX_FIELD_JSON_KEY, relativeIndex);
        builder.field(OLD_VALUES_FIELD_JSON_KEY, oldValues);
        builder.field(EXPECTED_VAL_LIST_FIELD_JSON_KEY, expectedValuesList);
        builder.field(THRESHOLD_FIELD_JSON_KEY, threshold);
        builder.endObject();
        return builder;
    }

}
