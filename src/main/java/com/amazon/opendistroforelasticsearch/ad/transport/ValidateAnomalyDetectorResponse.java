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

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.io.IOException;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import com.amazon.opendistroforelasticsearch.ad.model.DetectorValidationIssue;

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
        if (in.available() > 0) {
            issue = new DetectorValidationIssue(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (issue != null) {
            issue.writeTo(out);
        }
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
