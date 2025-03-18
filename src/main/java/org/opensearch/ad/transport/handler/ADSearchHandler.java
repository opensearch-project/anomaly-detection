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

package org.opensearch.ad.transport.handler;

import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.timeseries.transport.handler.SearchHandler;
import org.opensearch.transport.client.Client;

/**
 * Handle general search request, check user role and return search response.
 */
public class ADSearchHandler extends SearchHandler {

    public ADSearchHandler(Settings settings, ClusterService clusterService, Client client) {
        super(settings, clusterService, client, AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES);
    }
}
