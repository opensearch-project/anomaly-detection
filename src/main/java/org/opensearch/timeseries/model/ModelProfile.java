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

package org.opensearch.timeseries.model;

import java.io.IOException;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.constant.CommonName;

/**
 * Used to show model information in profile API
 *
 */
public class ModelProfile implements Writeable, ToXContentObject {
    private final String modelId;
    // added since Opensearch 1.1
    private final Entity entity;
    private final long modelSizeInBytes;

    public ModelProfile(String modelId, Entity entity, long modelSizeInBytes) {
        super();
        this.modelId = modelId;
        this.entity = entity;
        this.modelSizeInBytes = modelSizeInBytes;
    }

    public ModelProfile(StreamInput in) throws IOException {
        this.modelId = in.readString();
        if (in.readBoolean()) {
            this.entity = new Entity(in);
        } else {
            this.entity = null;
        }

        this.modelSizeInBytes = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        if (entity != null) {
            out.writeBoolean(true);
            entity.writeTo(out);
        } else {
            out.writeBoolean(false);
        }

        out.writeLong(modelSizeInBytes);
    }

    public String getModelId() {
        return modelId;
    }

    public Entity getEntity() {
        return entity;
    }

    public long getModelSizeInBytes() {
        return modelSizeInBytes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonName.MODEL_ID_FIELD, modelId);
        if (entity != null) {
            builder.field(CommonName.ENTITY_KEY, entity);
        }
        if (modelSizeInBytes > 0) {
            builder.field(CommonName.MODEL_SIZE_IN_BYTES, modelSizeInBytes);
        }
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        if (obj instanceof ModelProfile) {
            ModelProfile other = (ModelProfile) obj;
            EqualsBuilder equalsBuilder = new EqualsBuilder();
            equalsBuilder.append(modelId, other.modelId);

            return equalsBuilder.isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(modelId).toHashCode();
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.append(CommonName.MODEL_ID_FIELD, modelId);
        if (modelSizeInBytes > 0) {
            builder.append(CommonName.MODEL_SIZE_IN_BYTES, modelSizeInBytes);
        }
        if (entity != null) {
            builder.append(CommonName.ENTITY_KEY, entity);
        }
        return builder.toString();
    }
}
