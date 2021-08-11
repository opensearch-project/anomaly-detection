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

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class PMMLResultResponse extends ActionResponse implements ToXContentObject {
    public static final String OUTLIER_JSON_KEY = "outlier";
    public static final String DECISION_FUNCTION_JSON_KEY = "decisionFunction";

    public boolean outlier;
    public double decisionFunction;

    public PMMLResultResponse(boolean outlier, double decisionFunction) {
        this.outlier = outlier;
        this.decisionFunction = decisionFunction;
    }

    public PMMLResultResponse(StreamInput in) throws IOException {
        super(in);
        outlier = in.readBoolean();
        decisionFunction = in.readDouble();
    }

    public boolean getOutlier() {
        return outlier;
    }

    public double getDecisionFunction() {
        return decisionFunction;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(outlier);
        out.writeDouble(decisionFunction);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(OUTLIER_JSON_KEY, outlier);
        builder.field(DECISION_FUNCTION_JSON_KEY, decisionFunction);
        builder.endObject();
        return builder;
    }
}
