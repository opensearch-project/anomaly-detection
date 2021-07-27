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
import java.util.List;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class ADTaskProfileNodeResponse extends BaseNodeResponse {

    private List<ADTaskProfile> adTaskProfiles;

    public ADTaskProfileNodeResponse(DiscoveryNode node, List<ADTaskProfile> adTaskProfile) {
        super(node);
        this.adTaskProfiles = adTaskProfile;
    }

    public ADTaskProfileNodeResponse(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            this.adTaskProfiles = in.readList(ADTaskProfile::new);
        } else {
            this.adTaskProfiles = null;
        }
    }

    public List<ADTaskProfile> getAdTaskProfiles() {
        return adTaskProfiles;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (adTaskProfiles != null) {
            out.writeBoolean(true);
            out.writeList(adTaskProfiles);
        } else {
            out.writeBoolean(false);
        }
    }

    public static ADTaskProfileNodeResponse readNodeResponse(StreamInput in) throws IOException {
        return new ADTaskProfileNodeResponse(in);
    }
}
