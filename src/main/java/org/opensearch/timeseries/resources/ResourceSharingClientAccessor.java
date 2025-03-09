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

package org.opensearch.timeseries.resources;

import org.opensearch.common.settings.Settings;
import org.opensearch.security.client.resources.ResourceSharingNodeClient;
import org.opensearch.transport.client.node.NodeClient;

public class ResourceSharingClientAccessor {
    private static ResourceSharingNodeClient INSTANCE;

    private ResourceSharingClientAccessor() {}

    /**
     * Get resource sharing client
     *
     * @param nodeClient node client
     * @return resource sharing client
     */
    public static ResourceSharingNodeClient getResourceSharingClient(NodeClient nodeClient, Settings settings) {
        if (INSTANCE == null) {
            INSTANCE = new ResourceSharingNodeClient(nodeClient, settings);
        }
        return INSTANCE;
    }
}
