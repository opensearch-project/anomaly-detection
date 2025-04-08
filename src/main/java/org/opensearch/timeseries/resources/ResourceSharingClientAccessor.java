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

import org.opensearch.security.spi.resources.client.NoopResourceSharingClient;
import org.opensearch.security.spi.resources.client.ResourceSharingClient;

/**
 * Accessor for resource sharing client
 */
public class ResourceSharingClientAccessor {
    private static ResourceSharingClient CLIENT;

    private ResourceSharingClientAccessor() {}

    /**
     * Get resource sharing client
     *
     * @return resource sharing client, NoopResourceSharingClient when security is disabled
     */
    public static ResourceSharingClient getResourceSharingClient() {
        return CLIENT == null ? new NoopResourceSharingClient() : CLIENT;
    }

    /**
     * Set resource sharing client
     *
     * @param client resource sharing client
     */
    public static void setResourceSharingClient(ResourceSharingClient client) {
        CLIENT = client;
    }

}
