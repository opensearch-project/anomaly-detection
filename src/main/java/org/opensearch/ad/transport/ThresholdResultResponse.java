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

import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

public class ThresholdResultResponse extends ActionResponse implements ToXContentObject {
    private double anomalyGrade;
    private double confidence;

    public ThresholdResultResponse(double anomalyGrade, double confidence) {
        this.anomalyGrade = anomalyGrade;
        this.confidence = confidence;
    }

    public ThresholdResultResponse(StreamInput in) throws IOException {
        super(in);
        anomalyGrade = in.readDouble();
        confidence = in.readDouble();
    }

    public double getAnomalyGrade() {
        return anomalyGrade;
    }

    public double getConfidence() {
        return confidence;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(anomalyGrade);
        out.writeDouble(confidence);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ADCommonName.ANOMALY_GRADE_JSON_KEY, anomalyGrade);
        builder.field(ADCommonName.CONFIDENCE_JSON_KEY, confidence);
        builder.endObject();
        return builder;
    }

}
