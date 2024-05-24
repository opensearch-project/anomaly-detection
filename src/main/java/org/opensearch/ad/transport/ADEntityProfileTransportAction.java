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

import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.transport.BaseEntityProfileTransportAction;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Transport action to get entity profile.
 */
public class ADEntityProfileTransportAction extends
    BaseEntityProfileTransportAction<ThresholdedRandomCutForest, ADPriorityCache, ADCacheProvider> {

    @Inject
    public ADEntityProfileTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        Settings settings,
        HashRing hashRing,
        ClusterService clusterService,
        ADCacheProvider cacheProvider
    ) {
        super(
            actionFilters,
            transportService,
            settings,
            hashRing,
            clusterService,
            cacheProvider,
            ADEntityProfileAction.NAME,
            AnomalyDetectorSettings.AD_REQUEST_TIMEOUT
        );
    }
}
