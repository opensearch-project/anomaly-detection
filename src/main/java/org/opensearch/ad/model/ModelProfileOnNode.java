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

package org.opensearch.ad.model;

import java.io.IOException;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

public class ModelProfileOnNode implements Writeable, ToXContent {
    // field name in toXContent
    public static final String NODE_ID = "node_id";

    private final String nodeId;
    private final ModelProfile modelProfile;

    public ModelProfileOnNode(String nodeId, ModelProfile modelProfile) {
        this.nodeId = nodeId;
        this.modelProfile = modelProfile;
    }

    public ModelProfileOnNode(StreamInput in) throws IOException {
        this.nodeId = in.readString();
        this.modelProfile = new ModelProfile(in);
    }

    public String getModelId() {
        return modelProfile.getModelId();
    }

    public long getModelSize() {
        return modelProfile.getModelSizeInBytes();
    }

    public String getNodeId() {
        return nodeId;
    }

    public ModelProfile getModelProfile() {
        return modelProfile;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        modelProfile.toXContent(builder, params);
        builder.field(NODE_ID, nodeId);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        modelProfile.writeTo(out);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        if (obj instanceof ModelProfileOnNode) {
            ModelProfileOnNode other = (ModelProfileOnNode) obj;
            EqualsBuilder equalsBuilder = new EqualsBuilder();
            equalsBuilder.append(modelProfile, other.modelProfile);
            equalsBuilder.append(nodeId, other.nodeId);

            return equalsBuilder.isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(modelProfile).append(nodeId).toHashCode();
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        builder.append(ADCommonName.MODEL, modelProfile);
        builder.append(NODE_ID, nodeId);
        return builder.toString();
    }
}
