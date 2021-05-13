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
import java.time.Instant;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class PreviewAnomalyDetectorRequest extends ActionRequest {

    private AnomalyDetector detector;
    private String detectorId;
    private Instant startTime;
    private Instant endTime;

    public PreviewAnomalyDetectorRequest(StreamInput in) throws IOException {
        super(in);
        detector = new AnomalyDetector(in);
        detectorId = in.readOptionalString();
        startTime = in.readInstant();
        endTime = in.readInstant();
    }

    public PreviewAnomalyDetectorRequest(AnomalyDetector detector, String detectorId, Instant startTime, Instant endTime)
        throws IOException {
        super();
        this.detector = detector;
        this.detectorId = detectorId;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public String getDetectorId() {
        return detectorId;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        detector.writeTo(out);
        out.writeOptionalString(detectorId);
        out.writeInstant(startTime);
        out.writeInstant(endTime);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
