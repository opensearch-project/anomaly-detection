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

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.io.IOException;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonMessageAttributes;

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
        builder.field(CommonMessageAttributes.ANOMALY_GRADE_JSON_KEY, anomalyGrade);
        builder.field(CommonMessageAttributes.CONFIDENCE_JSON_KEY, confidence);
        builder.endObject();
        return builder;
    }

}
