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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.util.Bwc;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class EntityResultRequest extends ActionRequest implements ToXContentObject {
    private static final Logger LOG = LogManager.getLogger(EntityResultRequest.class);
    private String detectorId;
    // changed from Map<String, double[]> to Map<Entity, double[]>
    private Map<Entity, double[]> entities;
    private long start;
    private long end;

    public EntityResultRequest(StreamInput in) throws IOException {
        super(in);
        this.detectorId = in.readString();

        // guarded with version check. Just in case we receive requests from older node where we use String
        // to represent an entity
        if (Bwc.supportMultiCategoryFields(in.getVersion())) {
            this.entities = in.readMap(Entity::new, StreamInput::readDoubleArray);
        } else {
            // receive a request from a version before OpenSearch 1.1
            // the old request uses Map<String, double[]> instead of Map<Entity, double[]> to represent entities
            // since it only supports one categorical field.
            Map<String, double[]> oldFormatEntities = in.readMap(StreamInput::readString, StreamInput::readDoubleArray);
            entities = new HashMap<>();
            for (Map.Entry<String, double[]> entry : oldFormatEntities.entrySet()) {
                // we don't know the category field name as we don't have access to detector config object
                // so we put empty string as the category field name for now. Will handle the case
                // in EntityResultTransportAciton.
                entities.put(Entity.createSingleAttributeEntity(CommonName.EMPTY_FIELD, entry.getKey()), entry.getValue());
            }
        }

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
        // guarded with version check. Just in case we send requests to older node where we use String
        // to represent an entity
        if (Bwc.supportMultiCategoryFields(out.getVersion())) {
            out.writeMap(entities, (s, e) -> e.writeTo(s), StreamOutput::writeDoubleArray);
        } else {
            Map<String, double[]> oldFormatEntities = new HashMap<>();
            for (Map.Entry<Entity, double[]> entry : entities.entrySet()) {
                Map<String, String> attributes = entry.getKey().getAttributes();
                if (attributes.size() != 1) {
                    // cannot send a multi-category field entity to old node since it will
                    // cause EOF exception and stop the detector. The issue
                    // is temporary and will be gone after upgrade completes.
                    // Since one EntityResultRequest is sent to one node, we can safely
                    // ignore the rest of the requests.
                    LOG.info("Skip sending multi-category entities to an incompatible node. Attributes: ", attributes);
                    break;
                }
                oldFormatEntities.put(entry.getKey().getAttributes().entrySet().iterator().next().getValue(), entry.getValue());
            }
            out.writeMap(oldFormatEntities, StreamOutput::writeString, StreamOutput::writeDoubleArray);
        }

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
