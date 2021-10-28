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
    private double rcfScore;
    private double confidence;
    private int forestSize;
    private double[] attribution;
    private long totalUpdates = 0;
    private Version remoteAdVersion;
    private double anomalyGrade;

    public RCFResultResponse(
        double rcfScore,
        double confidence,
        int forestSize,
        double[] attribution,
        long totalUpdates,
        double grade,
        Version remoteAdVersion
    ) {
        this.rcfScore = rcfScore;
        this.confidence = confidence;
        this.forestSize = forestSize;
        this.attribution = attribution;
        this.totalUpdates = totalUpdates;
        this.anomalyGrade = grade;
        this.remoteAdVersion = remoteAdVersion;
    }

    public RCFResultResponse(StreamInput in) throws IOException {
        super(in);
        rcfScore = in.readDouble();
        confidence = in.readDouble();
        forestSize = in.readVInt();
        attribution = in.readDoubleArray();
        if (in.available() > 0) {
            totalUpdates = in.readLong();
            anomalyGrade = in.readDouble();
        }
    }

    public double getRCFScore() {
        return rcfScore;
    }

    public double getConfidence() {
        return confidence;
    }

    public int getForestSize() {
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

    public long getTotalUpdates() {
        return totalUpdates;
    }

    public double getAnomalyGrade() {
        return anomalyGrade;
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
        builder.endObject();
        return builder;
    }

}
