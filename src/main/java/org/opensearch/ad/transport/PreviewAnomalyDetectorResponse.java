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

import java.io.IOException;
import java.util.List;

import org.opensearch.action.ActionResponse;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class PreviewAnomalyDetectorResponse extends ActionResponse implements ToXContentObject {
    public static final String ANOMALY_RESULT = "anomaly_result";
    public static final String ANOMALY_DETECTOR = "anomaly_detector";
    private List<AnomalyResult> anomalyResult;
    private AnomalyDetector detector;

    public PreviewAnomalyDetectorResponse(StreamInput in) throws IOException {
        super(in);
        anomalyResult = in.readList(AnomalyResult::new);
        detector = new AnomalyDetector(in);
    }

    public PreviewAnomalyDetectorResponse(List<AnomalyResult> anomalyResult, AnomalyDetector detector) {
        this.anomalyResult = anomalyResult;
        this.detector = detector;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(anomalyResult);
        detector.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(ANOMALY_RESULT, anomalyResult).field(ANOMALY_DETECTOR, detector).endObject();
    }
}
