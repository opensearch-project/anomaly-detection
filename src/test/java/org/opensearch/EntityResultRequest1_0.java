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
import java.util.Locale;
import java.util.Map;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class EntityResultRequest1_0 extends ActionRequest implements ToXContentObject {

    private String detectorId;
    private Map<String, double[]> entities;
    private long start;
    private long end;

    public EntityResultRequest1_0(StreamInput in) throws IOException {
        super(in);
        this.detectorId = in.readString();
        this.entities = in.readMap(StreamInput::readString, StreamInput::readDoubleArray);
        this.start = in.readLong();
        this.end = in.readLong();
    }

    public EntityResultRequest1_0(String detectorId, Map<String, double[]> entities, long start, long end) {
        super();
        this.detectorId = detectorId;
        this.entities = entities;
        this.start = start;
        this.end = end;
    }

    public String getDetectorId() {
        return this.detectorId;
    }

    public Map<String, double[]> getEntities() {
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
        out.writeMap(this.entities, StreamOutput::writeString, StreamOutput::writeDoubleArray);
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
        for (String entity : entities.keySet()) {
            builder.field(entity, entities.get(entity));
        }
        builder.endObject();
        return builder;
    }
}
