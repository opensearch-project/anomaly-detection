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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.timeseries.model.ProfileName;

/**
 * implements a request to obtain profiles about an AD detector
 */
public class ProfileRequest extends BaseNodesRequest<ProfileRequest> {

    private Set<ProfileName> profilesToBeRetrieved;
    private String configId;

    public ProfileRequest(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        profilesToBeRetrieved = new HashSet<ProfileName>();
        if (size != 0) {
            for (int i = 0; i < size; i++) {
                profilesToBeRetrieved.add(in.readEnum(ProfileName.class));
            }
        }
        configId = in.readString();
    }

    /**
     * Constructor
     *
     * @param configId config id
     * @param profilesToBeRetrieved profiles to be retrieved
     * @param nodes nodes of nodes' profiles to be retrieved
     */
    public ProfileRequest(String configId, Set<ProfileName> profilesToBeRetrieved, DiscoveryNode... nodes) {
        super(nodes);
        this.configId = configId;
        this.profilesToBeRetrieved = profilesToBeRetrieved;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(profilesToBeRetrieved.size());
        for (ProfileName profile : profilesToBeRetrieved) {
            out.writeEnum(profile);
        }
        out.writeString(configId);
    }

    public String getConfigId() {
        return configId;
    }

    /**
     * Get the set that tracks which profiles should be retrieved
     *
     * @return the set that contains the profile names marked for retrieval
     */
    public Set<ProfileName> getProfilesToBeRetrieved() {
        return profilesToBeRetrieved;
    }
}
