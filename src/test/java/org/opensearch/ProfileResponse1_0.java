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

package org.opensearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

/**
 * This class consists of the aggregated responses from the nodes
 */
public class ProfileResponse1_0 extends BaseNodesResponse<ProfileNodeResponse1_0> implements ToXContentFragment {
    // filed name in toXContent
    static final String COORDINATING_NODE = CommonName.COORDINATING_NODE;
    static final String SHINGLE_SIZE = CommonName.SHINGLE_SIZE;
    static final String TOTAL_SIZE = CommonName.TOTAL_SIZE_IN_BYTES;
    static final String ACTIVE_ENTITY = CommonName.ACTIVE_ENTITIES;
    static final String MODELS = CommonName.MODELS;
    static final String TOTAL_UPDATES = CommonName.TOTAL_UPDATES;

    private ModelProfile1_0[] modelProfile;
    private int shingleSize;
    private String coordinatingNode;
    private long totalSizeInBytes;
    private long activeEntities;
    private long totalUpdates;

    /**
     * Constructor
     *
     * @param in StreamInput
     * @throws IOException thrown when unable to read from stream
     */
    public ProfileResponse1_0(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        modelProfile = new ModelProfile1_0[size];
        for (int i = 0; i < size; i++) {
            modelProfile[i] = new ModelProfile1_0(in);
        }
        shingleSize = in.readInt();
        coordinatingNode = in.readString();
        totalSizeInBytes = in.readVLong();
        activeEntities = in.readVLong();
        totalUpdates = in.readVLong();
    }

    /**
     * Constructor
     *
     * @param clusterName name of cluster
     * @param nodes List of ProfileNodeResponse from nodes
     * @param failures List of failures from nodes
     */
    public ProfileResponse1_0(ClusterName clusterName, List<ProfileNodeResponse1_0> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
        totalSizeInBytes = 0L;
        activeEntities = 0L;
        totalUpdates = 0L;
        shingleSize = -1;
        List<ModelProfile1_0> modelProfileList = new ArrayList<>();
        for (ProfileNodeResponse1_0 response : nodes) {
            String curNodeId = response.getNode().getId();
            if (response.getShingleSize() >= 0) {
                coordinatingNode = curNodeId;
                shingleSize = response.getShingleSize();
            }
            if (response.getModelSize() != null) {
                for (Map.Entry<String, Long> entry : response.getModelSize().entrySet()) {
                    totalSizeInBytes += entry.getValue();
                    modelProfileList.add(new ModelProfile1_0(entry.getKey(), entry.getValue(), curNodeId));
                }
            }

            if (response.getActiveEntities() > 0) {
                activeEntities += response.getActiveEntities();
            }
            if (response.getTotalUpdates() > totalUpdates) {
                totalUpdates = response.getTotalUpdates();
            }
        }
        if (coordinatingNode == null) {
            coordinatingNode = "";
        }
        this.modelProfile = modelProfileList.toArray(new ModelProfile1_0[0]);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(modelProfile.length);
        for (ModelProfile1_0 profile : modelProfile) {
            profile.writeTo(out);
        }
        out.writeInt(shingleSize);
        out.writeString(coordinatingNode);
        out.writeVLong(totalSizeInBytes);
        out.writeVLong(activeEntities);
        out.writeVLong(totalUpdates);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<ProfileNodeResponse1_0> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public List<ProfileNodeResponse1_0> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(ProfileNodeResponse1_0::readProfiles);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(COORDINATING_NODE, coordinatingNode);
        builder.field(SHINGLE_SIZE, shingleSize);
        builder.field(TOTAL_SIZE, totalSizeInBytes);
        builder.field(ACTIVE_ENTITY, activeEntities);
        builder.field(TOTAL_UPDATES, totalUpdates);
        builder.startArray(MODELS);
        for (ModelProfile1_0 profile : modelProfile) {
            profile.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    public ModelProfile1_0[] getModelProfile() {
        return modelProfile;
    }

    public int getShingleSize() {
        return shingleSize;
    }

    public long getActiveEntities() {
        return activeEntities;
    }

    public long getTotalUpdates() {
        return totalUpdates;
    }

    public String getCoordinatingNode() {
        return coordinatingNode;
    }

    public long getTotalSizeInBytes() {
        return totalSizeInBytes;
    }
}
