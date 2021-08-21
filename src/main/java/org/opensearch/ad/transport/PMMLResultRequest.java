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
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class PMMLResultRequest extends ActionRequest implements ToXContentObject {
    private String adID;
    private String mlModelID;
    private String[] featureNames;
    private double[] featureValues;

    // Messages used for validation error
    public static final String INVALID_FEATURE_NAME_MSG = "feature name vector is empty (in pmml result request)";
    public static final String INVALID_FEATURE_MSG = "feature vector is empty (in pmml result request)";
    public static final String INVALID_LENGTH_MSG = "feature name vector and feature vector have different lengths";

    public PMMLResultRequest(String adID, String mlModelID, String[] featureNames, double[] featureValues) {
        super();
        this.adID = adID;
        this.mlModelID = mlModelID;
        this.featureNames = featureNames;
        this.featureValues = featureValues;
    }

    public PMMLResultRequest(StreamInput in) throws IOException {
        super(in);
        adID = in.readString();
        mlModelID = in.readString();
        int size1 = in.readVInt();
        featureNames = new String[size1];
        for (int i = 0; i < size1; i++) {
            featureNames[i] = in.readString();
        }
        int size2 = in.readVInt();
        featureValues = new double[size2];
        for (int i = 0; i < size2; i++) {
            featureValues[i] = in.readDouble();
        }
    }

    public String getAdID() {
        return adID;
    }

    public String getMlModelID() {
        return mlModelID;
    }

    public String[] getFeatureNames() {
        return featureNames;
    }

    public double[] getFeatureValues() {
        return featureValues;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(adID);
        out.writeString(mlModelID);
        out.writeVInt(featureNames.length);
        for (String name : featureNames) {
            out.writeString(name);
        }
        out.writeVInt(featureValues.length);
        for (double value : featureValues) {
            out.writeDouble(value);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (adID == null || Strings.isEmpty(adID)) {
            validationException = addValidationError(CommonErrorMessages.AD_ID_MISSING_MSG, validationException);
        }
        if (mlModelID == null || Strings.isEmpty(mlModelID)) {
            validationException = addValidationError(CommonErrorMessages.ML_MODEL_ID_MISSING_MSG, validationException);
        }
        if (featureNames == null || featureNames.length == 0) {
            validationException = addValidationError(PMMLResultRequest.INVALID_FEATURE_NAME_MSG, validationException);
        }
        if (featureValues == null || featureValues.length == 0) {
            validationException = addValidationError(PMMLResultRequest.INVALID_FEATURE_MSG, validationException);
        }
        if ((featureValues != null && featureNames != null) && featureNames.length != featureValues.length) {
            validationException = addValidationError(PMMLResultRequest.INVALID_LENGTH_MSG, validationException);
        }
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CommonName.ID_JSON_KEY, adID);
        builder.field(CommonName.ML_MODEL_ID_KEY, mlModelID);
        builder.startArray(CommonName.FEATURE_NAME_JSON_KEY);
        for (String name : featureNames) {
            builder.value(name);
        }
        builder.endArray();
        builder.startArray(CommonName.FEATURE_JSON_KEY);
        for (double feature : featureValues) {
            builder.value(feature);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
