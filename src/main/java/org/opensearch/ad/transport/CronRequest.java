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

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;

/**
 * Request should be sent from the handler logic of transport delete detector API
 *
 */
public class CronRequest extends BaseNodesRequest<CronRequest> {

    public CronRequest() {
        super((String[]) null);
    }

    public CronRequest(StreamInput in) throws IOException {
        super(in);
    }

    public CronRequest(DiscoveryNode... nodes) {
        super(nodes);
    }
}
