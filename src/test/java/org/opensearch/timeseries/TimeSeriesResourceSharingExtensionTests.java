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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

import java.util.Iterator;
import java.util.Set;

import org.hamcrest.MatcherAssert;
import org.mockito.Mock;
import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.security.spi.resources.ResourceProvider;
import org.opensearch.security.spi.resources.client.ResourceSharingClient;

public class TimeSeriesResourceSharingExtensionTests extends ADUnitTestCase {

    @Mock
    private ResourceSharingClient mockClient;

    private static String extractIndexFrom(Set<ResourceProvider> providers) {
        MatcherAssert.assertThat("providers should not be null", providers, is(not(nullValue())));
        MatcherAssert.assertThat("Expected exactly one provider", providers.size(), equalTo(1));
        Iterator<ResourceProvider> it = providers.iterator();
        MatcherAssert.assertThat(it.hasNext(), equalTo(true));
        return it.next().resourceIndexName();
    }

    public void testGetResourceProviders() {
        TimeSeriesResourceSharingExtension extension = new TimeSeriesResourceSharingExtension();
        Set<ResourceProvider> providers = extension.getResourceProviders();

        assertEquals("Should expose exactly two resource providers", 2, providers.size());

        ResourceProvider anomalyProvider = providers
            .stream()
            .filter(p -> AnomalyDetector.class.getCanonicalName().equals(p.resourceType()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("AnomalyDetector provider missing"));
        assertEquals(
            "AnomalyDetector provider should use the AD index",
            ADIndex.CONFIG.getIndexName(),
            anomalyProvider.resourceIndexName()
        );

        ResourceProvider forecastProvider = providers
            .stream()
            .filter(p -> Forecaster.class.getCanonicalName().equals(p.resourceType()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Forecaster provider missing"));
        assertEquals(
            "Forecaster provider should use the Forecast index",
            ForecastIndex.CONFIG.getIndexName(),
            forecastProvider.resourceIndexName()
        );
    }

    public void testAssignResourceSharingClient_setsClientOnAccessor() {
        TimeSeriesResourceSharingExtension ext = new TimeSeriesResourceSharingExtension();

        assertThat(ext.getResourceSharingClient(), is(nullValue()));

        ext.assignResourceSharingClient(mockClient);

        assertThat("Accessor should hold the client passed to extension", ext.getResourceSharingClient(), equalTo(mockClient));
    }

    public void testAssignResourceSharingClient_overwritesExistingClient() {
        TimeSeriesResourceSharingExtension ext = new TimeSeriesResourceSharingExtension();
        ResourceSharingClient first = mock(ResourceSharingClient.class);
        ResourceSharingClient second = mock(ResourceSharingClient.class);

        // Prime with the first client
        ext.assignResourceSharingClient(first);
        assertThat(ext.getResourceSharingClient(), equalTo(first));

        // Now assign a new one via the extension
        ext.assignResourceSharingClient(second);

        assertThat("Accessor should be updated to the new client", ext.getResourceSharingClient(), equalTo(second));
    }

    public void testGetResourceProviders_isDeterministicAcrossCalls() {
        TimeSeriesResourceSharingExtension ext = new TimeSeriesResourceSharingExtension();

        Set<ResourceProvider> first = ext.getResourceProviders();
        Set<ResourceProvider> second = ext.getResourceProviders();

        // Same contents
        assertThat(first, equalTo(second));

        // Extract and compare details for additional safety
        String idx1 = extractIndexFrom(first);
        String idx2 = extractIndexFrom(second);
        assertThat(idx1, equalTo(idx2));
    }
}
