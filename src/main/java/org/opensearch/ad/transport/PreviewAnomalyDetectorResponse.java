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
import java.util.List;

import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

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
