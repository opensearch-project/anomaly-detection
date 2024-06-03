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

package org.opensearch.forecast.transport.handler;

import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.timeseries.transport.handler.SearchHandler;

/**
 * Handle general search request, check user role and return search response.
 */
public class ForecastSearchHandler extends SearchHandler {

    public ForecastSearchHandler(Settings settings, ClusterService clusterService, Client client) {
        super(settings, clusterService, client, ForecastSettings.FORECAST_FILTER_BY_BACKEND_ROLES);
    }
}
