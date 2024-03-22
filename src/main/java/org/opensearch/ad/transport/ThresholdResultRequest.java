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

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.constant.CommonName;

public class ThresholdResultRequest extends ActionRequest implements ToXContentObject {
    private String adID;
    private String modelID;
    private double rcfScore;

    public ThresholdResultRequest(StreamInput in) throws IOException {
        super(in);
        adID = in.readString();
        modelID = in.readString();
        rcfScore = in.readDouble();
    }

    public ThresholdResultRequest(String adID, String modelID, double rcfScore) {
        super();
        this.adID = adID;
        this.modelID = modelID;
        this.rcfScore = rcfScore;
    }

    public double getRCFScore() {
        return rcfScore;
    }

    public String getAdID() {
        return adID;
    }

    public String getModelID() {
        return modelID;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(adID);
        out.writeString(modelID);
        out.writeDouble(rcfScore);
    }

    /**
     * Verify request parameter corresponds to our understanding of the data.
     * We don't verify whether rcfScore is less than 0 or not because this cannot happen.
     */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(adID)) {
            validationException = addValidationError(ADCommonMessages.AD_ID_MISSING_MSG, validationException);
        }
        if (Strings.isEmpty(modelID)) {
            validationException = addValidationError(ADCommonMessages.MODEL_ID_MISSING_MSG, validationException);
        }

        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ADCommonName.ID_JSON_KEY, adID);
        builder.field(CommonName.MODEL_ID_FIELD, modelID);
        builder.field(ADCommonName.RCF_SCORE_JSON_KEY, rcfScore);
        builder.endObject();
        return builder;
    }
}
