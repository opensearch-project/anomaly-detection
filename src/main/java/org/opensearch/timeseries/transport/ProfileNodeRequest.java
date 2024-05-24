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
import java.util.Set;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.transport.TransportRequest;

/**
 *  Class representing a nodes's profile request
 */
public class ProfileNodeRequest extends TransportRequest {
    private ProfileRequest request;

    public ProfileNodeRequest(StreamInput in) throws IOException {
        super(in);
        this.request = new ProfileRequest(in);
    }

    /**
     * Constructor
     *
     * @param request profile request
     */
    public ProfileNodeRequest(ProfileRequest request) {
        this.request = request;
    }

    public String getConfigId() {
        return request.getConfigId();
    }

    /**
     * Get the set that tracks which profiles should be retrieved
     *
     * @return the set that contains the profile names marked for retrieval
     */
    public Set<ProfileName> getProfilesToBeRetrieved() {
        return request.getProfilesToBeRetrieved();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }
}
