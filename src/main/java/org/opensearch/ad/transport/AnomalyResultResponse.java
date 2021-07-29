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

package org.opensearch.ad.transport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opensearch.action.ActionResponse;
import org.opensearch.ad.model.FeatureData;
import org.opensearch.ad.util.Bwc;
import org.opensearch.common.io.stream.InputStreamStreamInput;
import org.opensearch.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class AnomalyResultResponse extends ActionResponse implements ToXContentObject {
    public static final String ANOMALY_GRADE_JSON_KEY = "anomalyGrade";
    public static final String CONFIDENCE_JSON_KEY = "confidence";
    public static final String ANOMALY_SCORE_JSON_KEY = "anomalyScore";
    public static final String ERROR_JSON_KEY = "error";
    public static final String FEATURES_JSON_KEY = "features";
    public static final String FEATURE_VALUE_JSON_KEY = "value";
    public static final String RCF_TOTAL_UPDATES_KEY = "rcfTotalUpdates";
    public static final String DETECTOR_INTERVAL_IN_MINUTES_KEY = "detectorIntervalInMinutes";

    private double anomalyGrade;
    private double confidence;
    private double anomalyScore;
    private String error;
    private List<FeatureData> features;
    private Long rcfTotalUpdates;
    private Long detectorIntervalInMinutes;
    private Boolean isHCDetector;

    public AnomalyResultResponse(double anomalyGrade, double confidence, double anomalyScore, List<FeatureData> features) {
        this(anomalyGrade, confidence, anomalyScore, features, null, null, null, null);
    }

    public AnomalyResultResponse(
        double anomalyGrade,
        double confidence,
        double anomalyScore,
        List<FeatureData> features,
        Long rcfTotalUpdates,
        Long detectorIntervalInMinutes
    ) {
        this(anomalyGrade, confidence, anomalyScore, features, null, rcfTotalUpdates, detectorIntervalInMinutes, null);
    }

    public AnomalyResultResponse(
        double anomalyGrade,
        double confidence,
        double anomalyScore,
        List<FeatureData> features,
        String error,
        Long rcfTotalUpdates,
        Long detectorIntervalInMinutes
    ) {
        this(anomalyGrade, confidence, anomalyScore, features, error, rcfTotalUpdates, detectorIntervalInMinutes, null);
    }

    public AnomalyResultResponse(
        double anomalyGrade,
        double confidence,
        double anomalyScore,
        List<FeatureData> features,
        String error,
        Long rcfTotalUpdates,
        Long detectorIntervalInMinutes,
        Boolean isHCDetector
    ) {
        this.anomalyGrade = anomalyGrade;
        this.confidence = confidence;
        this.anomalyScore = anomalyScore;
        this.features = features;
        this.error = error;
        this.rcfTotalUpdates = rcfTotalUpdates;
        this.detectorIntervalInMinutes = detectorIntervalInMinutes;
        this.isHCDetector = isHCDetector;
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
        if (Bwc.supportMultiCategoryFields(in.getVersion())) {
            rcfTotalUpdates = in.readOptionalLong();
            detectorIntervalInMinutes = in.readOptionalLong();
            isHCDetector = in.readOptionalBoolean();
        } else {
            rcfTotalUpdates = 0L;
            detectorIntervalInMinutes = 0L;
            isHCDetector = false;
        }
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
        if (Bwc.supportMultiCategoryFields(out.getVersion())) {
            out.writeOptionalLong(rcfTotalUpdates);
            out.writeOptionalLong(detectorIntervalInMinutes);
            out.writeOptionalBoolean(isHCDetector);
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
        builder.field(RCF_TOTAL_UPDATES_KEY, rcfTotalUpdates);
        builder.field(DETECTOR_INTERVAL_IN_MINUTES_KEY, detectorIntervalInMinutes);
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
}
