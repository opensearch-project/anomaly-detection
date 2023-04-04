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

import java.io.IOException;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

public class ModelProfile1_0 implements Writeable, ToXContent {
    // field name in toXContent
    public static final String MODEL_ID = "model_id";
    public static final String MODEL_SIZE_IN_BYTES = "model_size_in_bytes";
    public static final String NODE_ID = "node_id";

    private final String modelId;
    private final long modelSizeInBytes;
    private final String nodeId;

    public ModelProfile1_0(String modelId, long modelSize, String nodeId) {
        super();
        this.modelId = modelId;
        this.modelSizeInBytes = modelSize;
        this.nodeId = nodeId;
    }

    public ModelProfile1_0(StreamInput in) throws IOException {
        modelId = in.readString();
        modelSizeInBytes = in.readLong();
        nodeId = in.readString();
    }

    public String getModelId() {
        return modelId;
    }

    public long getModelSize() {
        return modelSizeInBytes;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID, modelId);
        if (modelSizeInBytes > 0) {
            builder.field(MODEL_SIZE_IN_BYTES, modelSizeInBytes);
        }
        builder.field(NODE_ID, nodeId);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeLong(modelSizeInBytes);
        out.writeString(nodeId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        if (obj instanceof ModelProfile1_0) {
            ModelProfile1_0 other = (ModelProfile1_0) obj;
            EqualsBuilder equalsBuilder = new EqualsBuilder();
            equalsBuilder.append(modelId, other.modelId);
            equalsBuilder.append(modelSizeInBytes, other.modelSizeInBytes);
            equalsBuilder.append(nodeId, other.nodeId);

            return equalsBuilder.isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(modelId).append(modelSizeInBytes).append(nodeId).toHashCode();
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.append(MODEL_ID, modelId);
        if (modelSizeInBytes > 0) {
            builder.append(MODEL_SIZE_IN_BYTES, modelSizeInBytes);
        }
        builder.append(NODE_ID, nodeId);
        return builder.toString();
    }
}
