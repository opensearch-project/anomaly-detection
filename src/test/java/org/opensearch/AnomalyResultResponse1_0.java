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

package org.opensearch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opensearch.action.ActionResponse;
import org.opensearch.ad.model.FeatureData;
import org.opensearch.common.io.stream.InputStreamStreamInput;
import org.opensearch.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class AnomalyResultResponse1_0 extends ActionResponse implements ToXContentObject {
    public static final String ANOMALY_GRADE_JSON_KEY = "anomalyGrade";
    public static final String CONFIDENCE_JSON_KEY = "confidence";
    public static final String ANOMALY_SCORE_JSON_KEY = "anomalyScore";
    public static final String ERROR_JSON_KEY = "error";
    public static final String FEATURES_JSON_KEY = "features";
    public static final String FEATURE_VALUE_JSON_KEY = "value";

    private double anomalyGrade;
    private double confidence;
    private double anomalyScore;
    private String error;
    private List<FeatureData> features;

    public AnomalyResultResponse1_0(double anomalyGrade, double confidence, double anomalyScore, List<FeatureData> features) {
        this(anomalyGrade, confidence, anomalyScore, features, null);
    }

    public AnomalyResultResponse1_0(double anomalyGrade, double confidence, double anomalyScore, List<FeatureData> features, String error) {
        this.anomalyGrade = anomalyGrade;
        this.confidence = confidence;
        this.anomalyScore = anomalyScore;
        this.features = features;
        this.error = error;
    }

    public AnomalyResultResponse1_0(StreamInput in) throws IOException {
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(anomalyGrade);
        out.writeDouble(confidence);
        out.writeDouble(anomalyScore);
        out.writeVInt(features.size());
        for (FeatureData feature : features) {
            feature.writeTo(out);
        }
        if (error != null) {
            out.writeBoolean(true);
            out.writeString(error);
        } else {
            out.writeBoolean(false);
        }
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
        builder.endObject();
        return builder;
    }

    public static AnomalyResultResponse1_0 fromActionResponse(final ActionResponse actionResponse) {
        if (actionResponse instanceof AnomalyResultResponse1_0) {
            return (AnomalyResultResponse1_0) actionResponse;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionResponse.writeTo(osso);
            try (InputStreamStreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new AnomalyResultResponse1_0(input);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse ActionResponse into AnomalyResultResponse", e);
        }
    }
}
