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

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;

public class ValidateAnomalyDetectorRequest extends ActionRequest {

    private final AnomalyDetector detector;
    private final String validationType;
    private final Integer maxSingleEntityAnomalyDetectors;
    private final Integer maxMultiEntityAnomalyDetectors;
    private final Integer maxAnomalyFeatures;
    private final TimeValue requestTimeout;

    public ValidateAnomalyDetectorRequest(StreamInput in) throws IOException {
        super(in);
        detector = new AnomalyDetector(in);
        validationType = in.readString();
        maxSingleEntityAnomalyDetectors = in.readInt();
        maxMultiEntityAnomalyDetectors = in.readInt();
        maxAnomalyFeatures = in.readInt();
        requestTimeout = in.readTimeValue();
    }

    public ValidateAnomalyDetectorRequest(
        AnomalyDetector detector,
        String validationType,
        Integer maxSingleEntityAnomalyDetectors,
        Integer maxMultiEntityAnomalyDetectors,
        Integer maxAnomalyFeatures,
        TimeValue requestTimeout
    ) {
        this.detector = detector;
        this.validationType = validationType;
        this.maxSingleEntityAnomalyDetectors = maxSingleEntityAnomalyDetectors;
        this.maxMultiEntityAnomalyDetectors = maxMultiEntityAnomalyDetectors;
        this.maxAnomalyFeatures = maxAnomalyFeatures;
        this.requestTimeout = requestTimeout;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        detector.writeTo(out);
        out.writeString(validationType);
        out.writeInt(maxSingleEntityAnomalyDetectors);
        out.writeInt(maxMultiEntityAnomalyDetectors);
        out.writeInt(maxAnomalyFeatures);
        out.writeTimeValue(requestTimeout);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public String getValidationType() {
        return validationType;
    }

    public Integer getMaxSingleEntityAnomalyDetectors() {
        return maxSingleEntityAnomalyDetectors;
    }

    public Integer getMaxMultiEntityAnomalyDetectors() {
        return maxMultiEntityAnomalyDetectors;
    }

    public Integer getMaxAnomalyFeatures() {
        return maxAnomalyFeatures;
    }

    public TimeValue getRequestTimeout() {
        return requestTimeout;
    }
}
