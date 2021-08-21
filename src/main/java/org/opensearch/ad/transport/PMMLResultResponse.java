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
