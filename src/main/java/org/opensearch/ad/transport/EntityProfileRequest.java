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

package org.opensearch.ad.transport;

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.EntityProfileName;
import org.opensearch.ad.util.Bwc;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class EntityProfileRequest extends ActionRequest implements ToXContentObject {
    private static final Logger LOG = LogManager.getLogger(EntityProfileRequest.class);
    public static final String ENTITY = "entity";
    public static final String PROFILES = "profiles";
    private String adID;
    // changed from String to Entity since 1.1
    private Entity entityValue;
    private Set<EntityProfileName> profilesToCollect;

    public EntityProfileRequest(StreamInput in) throws IOException {
        super(in);
        adID = in.readString();
        if (Bwc.supportMultiCategoryFields(in.getVersion())) {
            entityValue = new Entity(in);
        } else {
            // entity profile involving an old node won't work. Read
            // EntityProfileTransportAction.doExecute for details. Read
            // a string to not cause EOF exception.
            // Cannot assign null to entityValue as old node has no logic to
            // deal with a null entity.
            String oldFormatEntityString = in.readString();
            entityValue = Entity.createSingleAttributeEntity(adID, CommonName.EMPTY_FIELD, oldFormatEntityString);
        }
        int size = in.readVInt();
        profilesToCollect = new HashSet<EntityProfileName>();
        if (size != 0) {
            for (int i = 0; i < size; i++) {
                profilesToCollect.add(in.readEnum(EntityProfileName.class));
            }
        }
    }

    public EntityProfileRequest(String adID, Entity entityValue, Set<EntityProfileName> profilesToCollect) {
        super();
        this.adID = adID;
        this.entityValue = entityValue;
        this.profilesToCollect = profilesToCollect;
    }

    public String getAdID() {
        return adID;
    }

    public Entity getEntityValue() {
        return entityValue;
    }

    public Set<EntityProfileName> getProfilesToCollect() {
        return profilesToCollect;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(adID);
        if (Bwc.supportMultiCategoryFields(out.getVersion())) {
            entityValue.writeTo(out);
        } else {
            // entity profile involving an old node won't work. Read
            // EntityProfileTransportAction.doExecute for details. Write
            // a string to not cause EOF exception.
            out.writeString(entityValue.toString());
        }
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
        if (entityValue == null) {
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
        builder.field(CommonName.ID_JSON_KEY, adID);
        builder.field(ENTITY, entityValue);
        builder.field(PROFILES, profilesToCollect);
        builder.endObject();
        return builder;
    }
}
