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

package org.opensearch;

import static org.opensearch.action.ValidateActions.addValidationError;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.EntityProfileName;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

public class EntityProfileRequest1_0 extends ActionRequest implements ToXContentObject {
    public static final String ENTITY = "entity";
    public static final String PROFILES = "profiles";
    private String adID;
    private String entityValue;
    private Set<EntityProfileName> profilesToCollect;

    public EntityProfileRequest1_0(StreamInput in) throws IOException {
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

    public EntityProfileRequest1_0(String adID, String entityValue, Set<EntityProfileName> profilesToCollect) {
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
        builder.field(CommonName.ID_JSON_KEY, adID);
        builder.field(ENTITY, entityValue);
        builder.field(PROFILES, profilesToCollect);
        builder.endObject();
        return builder;
    }
}
