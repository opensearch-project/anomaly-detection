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

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.transport.handler.ForecastSearchHandler;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class SearchForecasterTransportAction extends HandledTransportAction<SearchRequest, SearchResponse> {
    private ForecastSearchHandler searchHandler;

    @Inject
    public SearchForecasterTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ForecastSearchHandler searchHandler
    ) {
        super(SearchForecasterAction.NAME, transportService, actionFilters, SearchRequest::new);
        this.searchHandler = searchHandler;
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
        searchHandler.search(request, ForecastCommonName.FORECAST_RESOURCE_TYPE, listener);
    }
}
