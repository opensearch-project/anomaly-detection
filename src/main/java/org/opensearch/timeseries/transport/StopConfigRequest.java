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

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;

public class StopConfigRequest extends ActionRequest implements ToXContentObject {

    private String configID;

    public StopConfigRequest() {}

    public StopConfigRequest(StreamInput in) throws IOException {
        super(in);
        this.configID = in.readString();
    }

    public StopConfigRequest(String configID) {
        super();
        this.configID = configID;
    }

    public String getConfigID() {
        return configID;
    }

    public StopConfigRequest adID(String configID) {
        this.configID = configID;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(configID);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(configID)) {
            validationException = addValidationError(CommonMessages.CONFIG_ID_MISSING_MSG, validationException);
        }
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CommonName.CONFIG_ID_KEY, configID);
        builder.endObject();
        return builder;
    }

    public static StopConfigRequest fromActionRequest(final ActionRequest actionRequest) {
        if (actionRequest instanceof StopConfigRequest) {
            return (StopConfigRequest) actionRequest;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionRequest.writeTo(osso);
            try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new StopConfigRequest(input);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse ActionRequest into StopDetectorRequest", e);
        }
    }
}
