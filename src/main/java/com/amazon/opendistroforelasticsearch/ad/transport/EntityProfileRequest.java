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
import java.util.HashSet;
import java.util.Set;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonMessageAttributes;
import com.amazon.opendistroforelasticsearch.ad.model.EntityProfileName;

public class EntityProfileRequest extends ActionRequest implements ToXContentObject {
    public static final String ENTITY = "entity";
    public static final String PROFILES = "profiles";
    private String adID;
    private String entityValue;
    private Set<EntityProfileName> profilesToCollect;

    public EntityProfileRequest(StreamInput in) throws IOException {
        super(in);
        adID = in.readString();
        entityValue = in.readString();
        int size = in.readVInt();
        profilesToCollect = new HashSet<EntityProfileName>();
        if (size != 0) {
            for (int i = 0; i < size; i++) {
                profilesToCollect.add(in.readEnum(EntityProfileName.class));
            }
        }
    }

    public EntityProfileRequest(String adID, String entityValue, Set<EntityProfileName> profilesToCollect) {
        super();
        this.adID = adID;
        this.entityValue = entityValue;
        this.profilesToCollect = profilesToCollect;
    }

    public String getAdID() {
        return adID;
    }

    public String getEntityValue() {
        return entityValue;
    }

    public Set<EntityProfileName> getProfilesToCollect() {
        return profilesToCollect;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(adID);
        out.writeString(entityValue);
        out.writeVInt(profilesToCollect.size());
        for (EntityProfileName profile : profilesToCollect) {
            out.writeEnum(profile);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(adID)) {
            validationException = addValidationError(CommonErrorMessages.AD_ID_MISSING_MSG, validationException);
        }
        if (Strings.isEmpty(entityValue)) {
            validationException = addValidationError("Entity value is missing", validationException);
        }
        if (profilesToCollect == null || profilesToCollect.isEmpty()) {
            validationException = addValidationError(CommonErrorMessages.EMPTY_PROFILES_COLLECT, validationException);
        }
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CommonMessageAttributes.ID_JSON_KEY, adID);
        builder.field(ENTITY, entityValue);
        builder.field(PROFILES, profilesToCollect);
        builder.endObject();
        return builder;
    }
}
