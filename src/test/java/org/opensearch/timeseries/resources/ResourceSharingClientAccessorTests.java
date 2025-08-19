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

import static org.mockito.Mockito.mock;

import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.security.spi.resources.client.ResourceSharingClient;

public class ResourceSharingClientAccessorTests extends ADUnitTestCase {

    private ResourceSharingClientAccessor accessor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        accessor = ResourceSharingClientAccessor.getInstance();
        accessor.setResourceSharingClient(null);
    }

    public void testSingletonInstance() {
        ResourceSharingClientAccessor first = ResourceSharingClientAccessor.getInstance();
        ResourceSharingClientAccessor second = ResourceSharingClientAccessor.getInstance();
        assertSame("getInstance() should always return the same accessor instance", first, second);
    }

    public void testSetAndGetResourceSharingClient() {
        ResourceSharingClient mockClient = mock(ResourceSharingClient.class);

        accessor.setResourceSharingClient(mockClient);

        ResourceSharingClient retrieved = accessor.getResourceSharingClient();
        assertSame("getResourceSharingClient() must return the exact instance previously set", mockClient, retrieved);
    }

    public void testGetResourceSharingClientWhenNotSet() {
        assertNull("When no client has been set, getResourceSharingClient() should return null", accessor.getResourceSharingClient());
    }
}
