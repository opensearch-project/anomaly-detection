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

package com.amazon.opendistroforelasticsearch.ad.transport;

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonMessageAttributes;

public class RCFResultRequest extends ActionRequest implements ToXContentObject {
    private String adID;
    private String modelID;
    private double[] features;

    // Messages used for validation error
    public static final String INVALID_FEATURE_MSG = "feature vector is empty";

    public RCFResultRequest(StreamInput in) throws IOException {
        super(in);
        adID = in.readString();
        modelID = in.readString();
        int size = in.readVInt();
        features = new double[size];
        for (int i = 0; i < size; i++) {
            features[i] = in.readDouble();
        }
    }

    public RCFResultRequest(String adID, String modelID, double[] features) {
        super();
        this.adID = adID;
        this.modelID = modelID;
        this.features = features;
    }

    public double[] getFeatures() {
        return features;
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
        out.writeVInt(features.length);
        for (double feature : features) {
            out.writeDouble(feature);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (features == null || features.length == 0) {
            validationException = addValidationError(RCFResultRequest.INVALID_FEATURE_MSG, validationException);
        }
        if (Strings.isEmpty(adID)) {
            validationException = addValidationError(CommonErrorMessages.AD_ID_MISSING_MSG, validationException);
        }
        if (Strings.isEmpty(modelID)) {
            validationException = addValidationError(CommonErrorMessages.MODEL_ID_MISSING_MSG, validationException);
        }
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CommonMessageAttributes.ID_JSON_KEY, adID);
        builder.field(CommonMessageAttributes.MODEL_ID_JSON_KEY, modelID);
        builder.startArray(CommonMessageAttributes.FEATURE_JSON_KEY);
        for (double feature : features) {
            builder.value(feature);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
