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
import java.util.Set;

import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.ad.model.DetectorProfileName;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

/**
 *  Class representing a nodes's profile request
 */
public class ProfileNodeRequest extends BaseNodeRequest {
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

    public String getDetectorId() {
        return request.getDetectorId();
    }

    /**
     * Get the set that tracks which profiles should be retrieved
     *
     * @return the set that contains the profile names marked for retrieval
     */
    public Set<DetectorProfileName> getProfilesToBeRetrieved() {
        return request.getProfilesToBeRetrieved();
    }

    /**
    *
    * @return Whether this is about a multi-entity detector or not
    */
    public boolean isForMultiEntityDetector() {
        return request.isForMultiEntityDetector();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }
}
