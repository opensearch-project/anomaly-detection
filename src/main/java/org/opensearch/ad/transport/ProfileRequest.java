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
import java.util.HashSet;
import java.util.Set;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.ad.model.DetectorProfileName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * implements a request to obtain profiles about an AD detector
 */
public class ProfileRequest extends BaseNodesRequest<ProfileRequest> {

    private Set<DetectorProfileName> profilesToBeRetrieved;
    private String detectorId;
    private boolean forMultiEntityDetector;

    public ProfileRequest(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        profilesToBeRetrieved = new HashSet<DetectorProfileName>();
        if (size != 0) {
            for (int i = 0; i < size; i++) {
                profilesToBeRetrieved.add(in.readEnum(DetectorProfileName.class));
            }
        }
        detectorId = in.readString();
        forMultiEntityDetector = in.readBoolean();
    }

    /**
     * Constructor
     *
     * @param detectorId detector's id
     * @param profilesToBeRetrieved profiles to be retrieved
     * @param forMultiEntityDetector whether the request is for a multi-entity detector
     * @param nodes nodes of nodes' profiles to be retrieved
     */
    public ProfileRequest(
        String detectorId,
        Set<DetectorProfileName> profilesToBeRetrieved,
        boolean forMultiEntityDetector,
        DiscoveryNode... nodes
    ) {
        super(nodes);
        this.detectorId = detectorId;
        this.profilesToBeRetrieved = profilesToBeRetrieved;
        this.forMultiEntityDetector = forMultiEntityDetector;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(profilesToBeRetrieved.size());
        for (DetectorProfileName profile : profilesToBeRetrieved) {
            out.writeEnum(profile);
        }
        out.writeString(detectorId);
        out.writeBoolean(forMultiEntityDetector);
    }

    public String getId() {
        return detectorId;
    }

    /**
     * Get the set that tracks which profiles should be retrieved
     *
     * @return the set that contains the profile names marked for retrieval
     */
    public Set<DetectorProfileName> getProfilesToBeRetrieved() {
        return profilesToBeRetrieved;
    }

    /**
     *
     * @return Whether this is about a multi-entity detector or not
     */
    public boolean isForMultiEntityDetector() {
        return forMultiEntityDetector;
    }
}
