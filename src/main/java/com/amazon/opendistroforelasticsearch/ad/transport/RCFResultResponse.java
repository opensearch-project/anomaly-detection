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

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class RCFResultResponse extends ActionResponse implements ToXContentObject {
    public static final String RCF_SCORE_JSON_KEY = "rcfScore";
    public static final String CONFIDENCE_JSON_KEY = "confidence";
    public static final String FOREST_SIZE_JSON_KEY = "forestSize";
    public static final String ATTRIBUTION_JSON_KEY = "attribution";
    private double rcfScore;
    private double confidence;
    private int forestSize;
    private double[] attribution;

    public RCFResultResponse(double rcfScore, double confidence, int forestSize, double[] attribution) {
        this.rcfScore = rcfScore;
        this.confidence = confidence;
        this.forestSize = forestSize;
        this.attribution = attribution;
    }

    public RCFResultResponse(StreamInput in) throws IOException {
        super(in);
        rcfScore = in.readDouble();
        confidence = in.readDouble();
        forestSize = in.readVInt();
        attribution = in.readDoubleArray();
    }

    public double getRCFScore() {
        return rcfScore;
    }

    public double getConfidence() {
        return confidence;
    }

    public int getForestSize() {
        return forestSize;
    }

    /**
     * Returns RCF score attribution.
     *
     * @return RCF score attribution.
     */
    public double[] getAttribution() {
        return attribution;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(rcfScore);
        out.writeDouble(confidence);
        out.writeVInt(forestSize);
        out.writeDoubleArray(attribution);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RCF_SCORE_JSON_KEY, rcfScore);
        builder.field(CONFIDENCE_JSON_KEY, confidence);
        builder.field(FOREST_SIZE_JSON_KEY, forestSize);
        builder.field(ATTRIBUTION_JSON_KEY, attribution);
        builder.endObject();
        return builder;
    }

}
