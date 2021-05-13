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
