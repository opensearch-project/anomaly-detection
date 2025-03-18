/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public abstract class BaseSearchConfigInfoTransportAction extends
    HandledTransportAction<SearchConfigInfoRequest, SearchConfigInfoResponse> {
    private static final Logger LOG = LogManager.getLogger(BaseSearchConfigInfoTransportAction.class);
    private final Client client;

    public BaseSearchConfigInfoTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        String searchConfigActionName
    ) {
        super(searchConfigActionName, transportService, actionFilters, SearchConfigInfoRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, SearchConfigInfoRequest request, ActionListener<SearchConfigInfoResponse> actionListener) {
        String name = request.getName();
        String rawPath = request.getRawPath();
        ActionListener<SearchConfigInfoResponse> listener = wrapRestActionListener(actionListener, CommonMessages.FAIL_TO_GET_CONFIG_INFO);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            SearchRequest searchRequest = new SearchRequest().indices(CommonName.CONFIG_INDEX);
            if (rawPath.endsWith(RestHandlerUtils.COUNT)) {
                // Count detectors
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchRequest.source(searchSourceBuilder);
                client.search(searchRequest, new ActionListener<SearchResponse>() {

                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        SearchConfigInfoResponse response = new SearchConfigInfoResponse(
                            searchResponse.getHits().getTotalHits().value(),
                            false
                        );
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e.getClass() == IndexNotFoundException.class) {
                            // Anomaly Detectors index does not exist
                            // Could be that user is creating first detector
                            SearchConfigInfoResponse response = new SearchConfigInfoResponse(0, false);
                            listener.onResponse(response);
                        } else {
                            listener.onFailure(e);
                        }
                    }
                });
            } else {
                // Match name with existing detectors
                TermsQueryBuilder query = QueryBuilders.termsQuery("name.keyword", name);
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query);
                searchRequest.source(searchSourceBuilder);
                client.search(searchRequest, new ActionListener<SearchResponse>() {

                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        boolean nameExists = false;
                        nameExists = searchResponse.getHits().getTotalHits().value() > 0;
                        SearchConfigInfoResponse response = new SearchConfigInfoResponse(0, nameExists);
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e.getClass() == IndexNotFoundException.class) {
                            // Anomaly Detectors index does not exist
                            // Could be that user is creating first detector
                            SearchConfigInfoResponse response = new SearchConfigInfoResponse(0, false);
                            listener.onResponse(response);
                        } else {
                            listener.onFailure(e);
                        }
                    }
                });
            }
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }
}
