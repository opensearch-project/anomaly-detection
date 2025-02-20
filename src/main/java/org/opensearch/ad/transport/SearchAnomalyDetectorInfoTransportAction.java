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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.timeseries.transport.BaseSearchConfigInfoTransportAction;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class SearchAnomalyDetectorInfoTransportAction extends BaseSearchConfigInfoTransportAction {

    @Inject
    public SearchAnomalyDetectorInfoTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService
    ) {
        super(transportService, actionFilters, client, SearchAnomalyDetectorInfoAction.NAME);
    }
}
