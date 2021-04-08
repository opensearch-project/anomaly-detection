/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.transport;

import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.transport.handler.ADSearchHandler;

public class SearchAnomalyResultTransportAction extends HandledTransportAction<SearchRequest, SearchResponse> {
    private ADSearchHandler searchHandler;

    @Inject
    public SearchAnomalyResultTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ADSearchHandler searchHandler
    ) {
        super(SearchAnomalyResultAction.NAME, transportService, actionFilters, SearchRequest::new);
        this.searchHandler = searchHandler;
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
        searchHandler.search(request, listener);
    }

}
