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

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class AnomalyDetectorJobRequest extends ActionRequest {

    private String detectorID;
    private DetectionDateRange detectionDateRange;
    private boolean historical;
    private long seqNo;
    private long primaryTerm;
    private String rawPath;

    public AnomalyDetectorJobRequest(StreamInput in) throws IOException {
        super(in);
        detectorID = in.readString();
        seqNo = in.readLong();
        primaryTerm = in.readLong();
        rawPath = in.readString();
        if (in.readBoolean()) {
            detectionDateRange = new DetectionDateRange(in);
        }
        historical = in.readBoolean();
    }

    public AnomalyDetectorJobRequest(String detectorID, long seqNo, long primaryTerm, String rawPath) {
        this(detectorID, null, false, seqNo, primaryTerm, rawPath);
    }

    /**
     * Constructor function.
     *
     * The detectionDateRange and historical boolean can be passed in individually.
     * The historical flag is for stopping detector, the detectionDateRange is for
     * starting detector. It's ok if historical is true but detectionDateRange is
     * null.
     *
     * @param detectorID detector identifier
     * @param detectionDateRange detection date range
     * @param historical historical analysis or not
     * @param seqNo seq no
     * @param primaryTerm primary term
     * @param rawPath raw request path
     */
    public AnomalyDetectorJobRequest(
        String detectorID,
        DetectionDateRange detectionDateRange,
        boolean historical,
        long seqNo,
        long primaryTerm,
        String rawPath
    ) {
        super();
        this.detectorID = detectorID;
        this.detectionDateRange = detectionDateRange;
        this.historical = historical;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.rawPath = rawPath;
    }

    public String getDetectorID() {
        return detectorID;
    }

    public DetectionDateRange getDetectionDateRange() {
        return detectionDateRange;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public String getRawPath() {
        return rawPath;
    }

    public boolean isHistorical() {
        return historical;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(detectorID);
        out.writeLong(seqNo);
        out.writeLong(primaryTerm);
        out.writeString(rawPath);
        if (detectionDateRange != null) {
            out.writeBoolean(true);
            detectionDateRange.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeBoolean(historical);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
