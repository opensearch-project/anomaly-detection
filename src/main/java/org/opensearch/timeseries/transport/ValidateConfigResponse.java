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

package org.opensearch.timeseries.transport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.model.ConfigValidationIssue;

public class ValidateConfigResponse extends ActionResponse implements ToXContentObject {
    private ConfigValidationIssue issue;

    public ConfigValidationIssue getIssue() {
        return issue;
    }

    public ValidateConfigResponse(ConfigValidationIssue issue) {
        this.issue = issue;
    }

    public ValidateConfigResponse(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            issue = new ConfigValidationIssue(in);
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

    public static ValidateConfigResponse fromActionResponse(ActionResponse actionResponse, NamedWriteableRegistry namedWriteableRegistry) {
        if (actionResponse instanceof ValidateConfigResponse) {
            return (ValidateConfigResponse) actionResponse;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionResponse.writeTo(osso);
            try (
                StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()));
                NamedWriteableAwareStreamInput namedWriteableAwareInput = new NamedWriteableAwareStreamInput(input, namedWriteableRegistry)
            ) {
                return new ValidateConfigResponse(namedWriteableAwareInput);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to parse ActionResponse into ValidateConfigResponse", e);
        }
    }
}
