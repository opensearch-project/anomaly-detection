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

package org.opensearch.timeseries;

import static org.mockito.Mockito.mock;

import java.util.Set;

import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.security.spi.resources.ResourceProvider;
import org.opensearch.security.spi.resources.client.ResourceSharingClient;
import org.opensearch.timeseries.resources.ResourceSharingClientAccessor;

public class TimeSeriesResourceSharingExtensionTests extends ADUnitTestCase {

    private TimeSeriesResourceSharingExtension extension;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        extension = new TimeSeriesResourceSharingExtension();
        ResourceSharingClientAccessor.getInstance().setResourceSharingClient(null);
    }

    public void testGetResourceProviders() {
        Set<ResourceProvider> providers = extension.getResourceProviders();

        assertEquals("Should expose exactly two resource providers", 2, providers.size());

        ResourceProvider anomalyProvider = providers
            .stream()
            .filter(p -> ADCommonName.AD_RESOURCE_TYPE.equals(p.resourceType()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("AnomalyDetector provider missing"));
        assertEquals(
            "AnomalyDetector provider should use the AD index",
            ADIndex.CONFIG.getIndexName(),
            anomalyProvider.resourceIndexName()
        );

        ResourceProvider forecastProvider = providers
            .stream()
            .filter(p -> ADCommonName.AD_RESOURCE_TYPE.equals(p.resourceType()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Forecaster provider missing"));
        assertEquals(
            "Forecaster provider should use the Forecast index",
            ForecastIndex.CONFIG.getIndexName(),
            forecastProvider.resourceIndexName()
        );
    }

    public void testAssignResourceSharingClient() {
        ResourceSharingClient mockClient = mock(ResourceSharingClient.class);

        extension.assignResourceSharingClient(mockClient);

        ResourceSharingClient retrieved = ResourceSharingClientAccessor.getInstance().getResourceSharingClient();
        assertSame("Accessor must hold the exact instance passed in", mockClient, retrieved);
    }
}
