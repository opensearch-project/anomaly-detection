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
import org.opensearch.ad.model.DetectorValidationIssue;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

public class ValidateAnomalyDetectorResponse extends ActionResponse implements ToXContentObject {
    private DetectorValidationIssue issue;

    public DetectorValidationIssue getIssue() {
        return issue;
    }

    public ValidateAnomalyDetectorResponse(DetectorValidationIssue issue) {
        this.issue = issue;
    }

    public ValidateAnomalyDetectorResponse(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            issue = new DetectorValidationIssue(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (issue != null) {
            out.writeBoolean(true);
            issue.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        return toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        if (issue != null) {
            xContentBuilder.field(issue.getAspect().getName(), issue);
        }
        return xContentBuilder.endObject();
    }
}
