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

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.DocRequest;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.timeseries.constant.CommonMessages;

public class DeleteConfigRequest extends ActionRequest implements DocRequest {

    private String configID;
    private String configIndex;

    public DeleteConfigRequest(StreamInput in) throws IOException {
        super(in);
        this.configID = in.readString();
        this.configIndex = in.readString();
    }

    public DeleteConfigRequest(String configId, String configIndex) {
        super();
        this.configID = configId;
        this.configIndex = configIndex;
    }

    public String getConfigID() {
        return configID;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(configID);
        out.writeString(configIndex);
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
    public String index() {
        return configIndex;
    }

    @Override
    public String id() {
        return configID;
    }
}
