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

import org.opensearch.security.spi.resources.client.ResourceSharingClient;

/**
 * Accessor for resource sharing client
 */
public class ResourceSharingClientAccessor {
    private ResourceSharingClient CLIENT;

    private static ResourceSharingClientAccessor resourceSharingClientAccessor;

    private ResourceSharingClientAccessor() {}

    public static ResourceSharingClientAccessor getInstance() {
        if (resourceSharingClientAccessor == null) {
            resourceSharingClientAccessor = new ResourceSharingClientAccessor();
        }

        return resourceSharingClientAccessor;
    }

    /**
     * Set the resource sharing client
     */
    public void setResourceSharingClient(ResourceSharingClient client) {
        resourceSharingClientAccessor.CLIENT = client;
    }

    /**
     * Get the resource sharing client
     */
    public ResourceSharingClient getResourceSharingClient() {
        return resourceSharingClientAccessor.CLIENT;
    }
}
