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

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

public class StopConfigResponse extends ActionResponse implements ToXContentObject {
    public static final String SUCCESS_JSON_KEY = "success";
    private boolean success;

    public StopConfigResponse(boolean success) {
        this.success = success;
    }

    public StopConfigResponse(StreamInput in) throws IOException {
        super(in);
        success = in.readBoolean();
    }

    public boolean success() {
        return success;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(success);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SUCCESS_JSON_KEY, success);
        builder.endObject();
        return builder;
    }

    public static StopConfigResponse fromActionResponse(final ActionResponse actionResponse) {
        if (actionResponse instanceof StopConfigResponse) {
            return (StopConfigResponse) actionResponse;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionResponse.writeTo(osso);
            try (InputStreamStreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new StopConfigResponse(input);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse ActionResponse into StopDetectorResponse", e);
        }
    }
}
