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
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.Entity;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class EntityResultRequest extends ActionRequest implements ToXContentObject {
    private static final Logger LOG = LogManager.getLogger(EntityResultRequest.class);

    private String detectorId;
    private Map<Entity, double[]> entities;
    private long start;
    private long end;

    public EntityResultRequest(StreamInput in) throws IOException {
        super(in);
        this.detectorId = in.readString();
        this.entities = in.readMap(Entity::new, StreamInput::readDoubleArray);
        this.start = in.readLong();
        this.end = in.readLong();
    }

    public EntityResultRequest(String detectorId, Map<Entity, double[]> entities, long start, long end) {
        super();
        this.detectorId = detectorId;
        this.entities = entities;
        this.start = start;
        this.end = end;
    }

    public String getDetectorId() {
        return this.detectorId;
    }

    public Map<Entity, double[]> getEntities() {
        return this.entities;
    }

    public long getStart() {
        return this.start;
    }

    public long getEnd() {
        return this.end;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.detectorId);
        out.writeMap(entities, (s, e) -> e.writeTo(s), StreamOutput::writeDoubleArray);
        out.writeLong(this.start);
        out.writeLong(this.end);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(detectorId)) {
            validationException = addValidationError(CommonErrorMessages.AD_ID_MISSING_MSG, validationException);
        }
        if (start <= 0 || end <= 0 || start > end) {
            validationException = addValidationError(
                String.format(Locale.ROOT, "%s: start %d, end %d", CommonErrorMessages.INVALID_TIMESTAMP_ERR_MSG, start, end),
                validationException
            );
        }
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CommonName.ID_JSON_KEY, detectorId);
        builder.field(CommonName.START_JSON_KEY, start);
        builder.field(CommonName.END_JSON_KEY, end);
        builder.startArray(CommonName.ENTITIES_JSON_KEY);
        for (final Map.Entry<Entity, double[]> entry : entities.entrySet()) {
            if (entry.getKey() != null) {
                builder.startObject();
                builder.field(CommonName.ENTITY_KEY, entry.getKey());
                builder.field(CommonName.VALUE_JSON_KEY, entry.getValue());
                builder.endObject();
            }
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
