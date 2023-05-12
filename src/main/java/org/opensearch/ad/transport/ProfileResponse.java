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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.ModelProfile;
import org.opensearch.ad.model.ModelProfileOnNode;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * This class consists of the aggregated responses from the nodes
 */
public class ProfileResponse extends BaseNodesResponse<ProfileNodeResponse> implements ToXContentFragment {
    private static final Logger LOG = LogManager.getLogger(ProfileResponse.class);
    // filed name in toXContent
    static final String COORDINATING_NODE = ADCommonName.COORDINATING_NODE;
    static final String SHINGLE_SIZE = ADCommonName.SHINGLE_SIZE;
    static final String TOTAL_SIZE = ADCommonName.TOTAL_SIZE_IN_BYTES;
    static final String ACTIVE_ENTITY = ADCommonName.ACTIVE_ENTITIES;
    static final String MODELS = ADCommonName.MODELS;
    static final String TOTAL_UPDATES = ADCommonName.TOTAL_UPDATES;
    static final String MODEL_COUNT = ADCommonName.MODEL_COUNT;

    // changed from ModelProfile to ModelProfileOnNode since Opensearch 1.1
    private ModelProfileOnNode[] modelProfile;
    private int shingleSize;
    private String coordinatingNode;
    private long totalSizeInBytes;
    private long activeEntities;
    private long totalUpdates;
    // added since 1.1
    private long modelCount;

    /**
     * Constructor
     *
     * @param in StreamInput
     * @throws IOException thrown when unable to read from stream
     */
    public ProfileResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        modelProfile = new ModelProfileOnNode[size];
        for (int i = 0; i < size; i++) {
            modelProfile[i] = new ModelProfileOnNode(in);
        }

        shingleSize = in.readInt();
        coordinatingNode = in.readString();
        totalSizeInBytes = in.readVLong();
        activeEntities = in.readVLong();
        totalUpdates = in.readVLong();
        modelCount = in.readVLong();
    }

    /**
     * Constructor
     *
     * @param clusterName name of cluster
     * @param nodes List of ProfileNodeResponse from nodes
     * @param failures List of failures from nodes
     */
    public ProfileResponse(ClusterName clusterName, List<ProfileNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
        totalSizeInBytes = 0L;
        activeEntities = 0L;
        totalUpdates = 0L;
        shingleSize = -1;
        modelCount = 0;
        List<ModelProfileOnNode> modelProfileList = new ArrayList<>();
        for (ProfileNodeResponse response : nodes) {
            String curNodeId = response.getNode().getId();
            if (response.getShingleSize() >= 0) {
                coordinatingNode = curNodeId;
                shingleSize = response.getShingleSize();
            }
            if (response.getModelSize() != null) {
                for (Map.Entry<String, Long> entry : response.getModelSize().entrySet()) {
                    totalSizeInBytes += entry.getValue();
                }
            }
            if (response.getModelProfiles() != null && response.getModelProfiles().size() > 0) {
                modelCount += response.getModelCount();
                for (ModelProfile profile : response.getModelProfiles()) {
                    modelProfileList.add(new ModelProfileOnNode(curNodeId, profile));
                }
            } else if (response.getModelSize() != null && response.getModelSize().size() > 0) {
                for (Map.Entry<String, Long> entry : response.getModelSize().entrySet()) {
                    // single-stream detectors have no entity info
                    modelProfileList.add(new ModelProfileOnNode(curNodeId, new ModelProfile(entry.getKey(), null, entry.getValue())));
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
        this.modelProfile = modelProfileList.toArray(new ModelProfileOnNode[0]);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(modelProfile.length);

        for (ModelProfileOnNode profile : modelProfile) {
            profile.writeTo(out);
        }

        out.writeInt(shingleSize);
        out.writeString(coordinatingNode);
        out.writeVLong(totalSizeInBytes);
        out.writeVLong(activeEntities);
        out.writeVLong(totalUpdates);
        out.writeVLong(modelCount);
    }

    @Override
    public void writeNodesTo(StreamOutput out, List<ProfileNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public List<ProfileNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(ProfileNodeResponse::readProfiles);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(COORDINATING_NODE, coordinatingNode);
        builder.field(SHINGLE_SIZE, shingleSize);
        builder.field(TOTAL_SIZE, totalSizeInBytes);
        builder.field(ACTIVE_ENTITY, activeEntities);
        builder.field(TOTAL_UPDATES, totalUpdates);
        if (modelCount > 0) {
            builder.field(MODEL_COUNT, modelCount);
        }
        builder.startArray(MODELS);
        for (ModelProfileOnNode profile : modelProfile) {
            profile.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    public ModelProfileOnNode[] getModelProfile() {
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

    public long getModelCount() {
        return modelCount;
    }
}
