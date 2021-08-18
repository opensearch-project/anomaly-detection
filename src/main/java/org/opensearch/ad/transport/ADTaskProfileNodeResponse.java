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
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.ad.cluster.ADVersionUtil;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class ADTaskProfileNodeResponse extends BaseNodeResponse {
    private static final Logger logger = LogManager.getLogger(ADTaskProfileNodeResponse.class);
    private ADTaskProfile adTaskProfile;
    private Version remoteAdVersion;

    public ADTaskProfileNodeResponse(DiscoveryNode node, ADTaskProfile adTaskProfile, Version remoteAdVersion) {
        super(node);
        this.adTaskProfile = adTaskProfile;
        this.remoteAdVersion = remoteAdVersion;
    }

    public ADTaskProfileNodeResponse(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            adTaskProfile = new ADTaskProfile(in);
        } else {
            adTaskProfile = null;
        }
    }

    public ADTaskProfile getAdTaskProfile() {
        return adTaskProfile;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (adTaskProfile != null && (ADVersionUtil.versionCompatible(remoteAdVersion) || adTaskProfile.getNodeId() != null)) {
            out.writeBoolean(true);
            adTaskProfile.writeTo(out, remoteAdVersion);
        } else {
            out.writeBoolean(false);
        }
    }

    public static ADTaskProfileNodeResponse readNodeResponse(StreamInput in) throws IOException {
        return new ADTaskProfileNodeResponse(in);
    }
}
