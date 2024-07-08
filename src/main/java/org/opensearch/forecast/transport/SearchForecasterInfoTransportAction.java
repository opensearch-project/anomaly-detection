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

package org.opensearch.forecast.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.timeseries.transport.BaseSearchConfigInfoTransportAction;
import org.opensearch.transport.TransportService;

public class SearchForecasterInfoTransportAction extends BaseSearchConfigInfoTransportAction {

    @Inject
    public SearchForecasterInfoTransportAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(transportService, actionFilters, client, SearchForecasterInfoAction.NAME);
    }
}
