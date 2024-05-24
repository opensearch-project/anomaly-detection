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
import java.util.Locale;
import java.util.Map;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Entity;

public class EntityResultRequest extends ActionRequest implements ToXContentObject {
    protected String configId;
    // changed from Map<String, double[]> to Map<Entity, double[]>
    protected Map<Entity, double[]> entities;
    // data start/end time epoch
    protected long start;
    protected long end;
    protected AnalysisType analysisType;
    protected String taskId;

    public EntityResultRequest(StreamInput in) throws IOException {
        super(in);
        this.configId = in.readString();

        // guarded with version check. Just in case we receive requests from older node where we use String
        // to represent an entity
        this.entities = in.readMap(Entity::new, StreamInput::readDoubleArray);

        this.start = in.readLong();
        this.end = in.readLong();

        // newly added
        if (in.available() > 0) {
            analysisType = in.readEnum(AnalysisType.class);
            taskId = in.readOptionalString();
        }
    }

    public EntityResultRequest(
        String configId,
        Map<Entity, double[]> entities,
        long start,
        long end,
        AnalysisType analysisType,
        String taskId
    ) {
        super();
        this.configId = configId;
        this.entities = entities;
        this.start = start;
        this.end = end;
        this.analysisType = analysisType;
        this.taskId = taskId;
    }

    public String getConfigId() {
        return this.configId;
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

    public AnalysisType getAnalysisType() {
        return analysisType;
    }

    public String getTaskId() {
        return taskId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.configId);
        // guarded with version check. Just in case we send requests to older node where we use String
        // to represent an entity
        out.writeMap(entities, (s, e) -> e.writeTo(s), StreamOutput::writeDoubleArray);

        out.writeLong(this.start);
        out.writeLong(this.end);
        out.writeEnum(analysisType);
        out.writeOptionalString(taskId);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isEmpty(configId)) {
            validationException = addValidationError(CommonMessages.CONFIG_ID_MISSING_MSG, validationException);
        }
        if (start <= 0 || end <= 0 || start > end) {
            validationException = addValidationError(
                String.format(Locale.ROOT, "%s: start %d, end %d", CommonMessages.INVALID_TIMESTAMP_ERR_MSG, start, end),
                validationException
            );
        }
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CommonName.CONFIG_ID_KEY, configId);
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
        builder.field(CommonName.ANALYSIS_TYPE_FIELD, analysisType);
        builder.field(CommonName.TASK_ID_FIELD, taskId);
        builder.endObject();
        return builder;
    }
}
