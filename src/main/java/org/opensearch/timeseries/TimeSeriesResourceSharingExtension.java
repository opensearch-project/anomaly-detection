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

import java.util.Set;

import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.security.spi.resources.ResourceProvider;
import org.opensearch.security.spi.resources.ResourceSharingExtension;
import org.opensearch.security.spi.resources.client.ResourceSharingClient;
import org.opensearch.timeseries.resources.ResourceSharingClientAccessor;

public class TimeSeriesResourceSharingExtension implements ResourceSharingExtension {
    @Override
    public Set<ResourceProvider> getResourceProviders() {
        return Set.of(new ResourceProvider() {
            @Override
            public String resourceType() {
                return ADCommonName.AD_RESOURCE_TYPE;
            }

            @Override
            public String resourceIndexName() {
                return ADIndex.CONFIG.getIndexName();
            }
        }, new ResourceProvider() {
            @Override
            public String resourceType() {
                return ForecastCommonName.FORECAST_RESOURCE_TYPE;
            }

            @Override
            public String resourceIndexName() {
                return ForecastIndex.CONFIG.getIndexName();
            }
        });
    }

    @Override
    public void assignResourceSharingClient(ResourceSharingClient resourceSharingClient) {
        ResourceSharingClientAccessor.getInstance().setResourceSharingClient(resourceSharingClient);
    }
}
