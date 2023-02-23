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
import java.util.List;
import java.util.Map;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.ModelProfile;
import org.opensearch.ad.util.Bwc;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

/**
 * Profile response on a node
 */
public class ProfileNodeResponse extends BaseNodeResponse implements ToXContentFragment {
    private Map<String, Long> modelSize;
    private int shingleSize;
    private long activeEntities;
    private long totalUpdates;
    // added after OpenSearch 1.0
    private List<ModelProfile> modelProfiles;
    private long modelCount;

    /**
     * Constructor
     *
     * @param in StreamInput
     * @throws IOException throws an IO exception if the StreamInput cannot be read from
     */
    public ProfileNodeResponse(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            modelSize = in.readMap(StreamInput::readString, StreamInput::readLong);
        }
        shingleSize = in.readInt();
        activeEntities = in.readVLong();
        totalUpdates = in.readVLong();
        if (Bwc.supportMultiCategoryFields(in.getVersion()) && in.readBoolean()) {
            // added after OpenSearch 1.0
            modelProfiles = in.readList(ModelProfile::new);
            modelCount = in.readVLong();
        }
    }

    /**
     * Constructor
     *
     * @param node DiscoveryNode object
     * @param modelSize Mapping of model id to its memory consumption in bytes
     * @param shingleSize shingle size
     * @param activeEntity active entity count
     * @param totalUpdates RCF model total updates
     * @param modelProfiles a collection of model profiles like model size
     * @param modelCount the number of models on the node
     */
    public ProfileNodeResponse(
        DiscoveryNode node,
        Map<String, Long> modelSize,
        int shingleSize,
        long activeEntity,
        long totalUpdates,
        List<ModelProfile> modelProfiles,
        long modelCount
    ) {
        super(node);
        this.modelSize = modelSize;
        this.shingleSize = shingleSize;
        this.activeEntities = activeEntity;
        this.totalUpdates = totalUpdates;
        this.modelProfiles = modelProfiles;
        this.modelCount = modelCount;
    }

    /**
     * Creates a new ProfileNodeResponse object and reads in the profile from an input stream
     *
     * @param in StreamInput to read from
     * @return ProfileNodeResponse object corresponding to the input stream
     * @throws IOException throws an IO exception if the StreamInput cannot be read from
     */
    public static ProfileNodeResponse readProfiles(StreamInput in) throws IOException {
        return new ProfileNodeResponse(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (modelSize != null) {
            out.writeBoolean(true);
            out.writeMap(modelSize, StreamOutput::writeString, StreamOutput::writeLong);
        } else {
            out.writeBoolean(false);
        }

        out.writeInt(shingleSize);
        out.writeVLong(activeEntities);
        out.writeVLong(totalUpdates);
        if (Bwc.supportMultiCategoryFields(out.getVersion())) {
            // added after OpenSearch 1.0
            if (modelProfiles != null) {
                out.writeBoolean(true);
                out.writeList(modelProfiles);
                out.writeVLong(modelCount);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    /**
     * Converts profile to xContent
     *
     * @param builder XContentBuilder
     * @param params Params
     * @return XContentBuilder
     * @throws IOException thrown by builder for invalid field
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(CommonName.MODEL_SIZE_IN_BYTES);
        for (Map.Entry<String, Long> entry : modelSize.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();

        builder.field(CommonName.SHINGLE_SIZE, shingleSize);
        builder.field(CommonName.ACTIVE_ENTITIES, activeEntities);
        builder.field(CommonName.TOTAL_UPDATES, totalUpdates);

        builder.field(CommonName.MODEL_COUNT, modelCount);
        builder.startArray(CommonName.MODELS);
        for (ModelProfile modelProfile : modelProfiles) {
            builder.startObject();
            modelProfile.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();

        return builder;
    }

    public Map<String, Long> getModelSize() {
        return modelSize;
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

    public List<ModelProfile> getModelProfiles() {
        return modelProfiles;
    }

    public long getModelCount() {
        return modelCount;
    }
}
