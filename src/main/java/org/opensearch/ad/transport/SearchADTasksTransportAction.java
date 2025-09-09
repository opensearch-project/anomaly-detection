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

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.transport.handler.ADSearchHandler;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class SearchADTasksTransportAction extends HandledTransportAction<SearchRequest, SearchResponse> {
    private ADSearchHandler searchHandler;

    @Inject
    public SearchADTasksTransportAction(TransportService transportService, ActionFilters actionFilters, ADSearchHandler searchHandler) {
        super(SearchADTasksAction.NAME, transportService, actionFilters, SearchRequest::new);
        this.searchHandler = searchHandler;
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
        searchHandler.search(request, listener);
    }
}
