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

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.RestRequest;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;

public class IndexAnomalyDetectorRequest extends ActionRequest {

    private String detectorID;
    private long seqNo;
    private long primaryTerm;
    private WriteRequest.RefreshPolicy refreshPolicy;
    private AnomalyDetector detector;
    private RestRequest.Method method;
    private TimeValue requestTimeout;
    private Integer maxSingleEntityAnomalyDetectors;
    private Integer maxMultiEntityAnomalyDetectors;
    private Integer maxAnomalyFeatures;

    public IndexAnomalyDetectorRequest(StreamInput in) throws IOException {
        super(in);
        detectorID = in.readString();
        seqNo = in.readLong();
        primaryTerm = in.readLong();
        refreshPolicy = in.readEnum(WriteRequest.RefreshPolicy.class);
        detector = new AnomalyDetector(in);
        method = in.readEnum(RestRequest.Method.class);
        requestTimeout = in.readTimeValue();
        maxSingleEntityAnomalyDetectors = in.readInt();
        maxMultiEntityAnomalyDetectors = in.readInt();
        maxAnomalyFeatures = in.readInt();
    }

    public IndexAnomalyDetectorRequest(
        String detectorID,
        long seqNo,
        long primaryTerm,
        WriteRequest.RefreshPolicy refreshPolicy,
        AnomalyDetector detector,
        RestRequest.Method method,
        TimeValue requestTimeout,
        Integer maxSingleEntityAnomalyDetectors,
        Integer maxMultiEntityAnomalyDetectors,
        Integer maxAnomalyFeatures
    ) {
        super();
        this.detectorID = detectorID;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.refreshPolicy = refreshPolicy;
        this.detector = detector;
        this.method = method;
        this.requestTimeout = requestTimeout;
        this.maxSingleEntityAnomalyDetectors = maxSingleEntityAnomalyDetectors;
        this.maxMultiEntityAnomalyDetectors = maxMultiEntityAnomalyDetectors;
        this.maxAnomalyFeatures = maxAnomalyFeatures;
    }

    public String getDetectorID() {
        return detectorID;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public RestRequest.Method getMethod() {
        return method;
    }

    public TimeValue getRequestTimeout() {
        return requestTimeout;
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(detectorID);
        out.writeLong(seqNo);
        out.writeLong(primaryTerm);
        out.writeEnum(refreshPolicy);
        detector.writeTo(out);
        out.writeEnum(method);
        out.writeTimeValue(requestTimeout);
        out.writeInt(maxSingleEntityAnomalyDetectors);
        out.writeInt(maxMultiEntityAnomalyDetectors);
        out.writeInt(maxAnomalyFeatures);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
