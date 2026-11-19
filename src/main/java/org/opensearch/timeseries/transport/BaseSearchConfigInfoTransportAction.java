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
import org.opensearch.action.support.ContextPreservingActionListener;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.util.PluginClient;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public abstract class BaseSearchConfigInfoTransportAction extends
    HandledTransportAction<SearchConfigInfoRequest, SearchConfigInfoResponse> {
    private static final Logger LOG = LogManager.getLogger(BaseSearchConfigInfoTransportAction.class);
    private final Client client;
    private final PluginClient pluginClient;
    protected String configIndexName;

    /**
     * Creates the action.
     *
     * @param transportService transport service
     * @param actionFilters action filters
     * @param client node client, used for the name match across all configs
     * @param pluginClient plugin client, used for the count of configs visible to the caller
     * @param searchConfigActionName action name
     * @param configIndexName config index to search
     */
    public BaseSearchConfigInfoTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        PluginClient pluginClient,
        String searchConfigActionName,
        String configIndexName
    ) {
        super(searchConfigActionName, transportService, actionFilters, SearchConfigInfoRequest::new);
        this.client = client;
        this.pluginClient = pluginClient;
        this.configIndexName = configIndexName;
    }

    @Override
    protected void doExecute(Task task, SearchConfigInfoRequest request, ActionListener<SearchConfigInfoResponse> actionListener) {
        String name = request.getName();
        String rawPath = request.getRawPath();
        ActionListener<SearchConfigInfoResponse> listener = wrapRestActionListener(actionListener, CommonMessages.FAIL_TO_GET_CONFIG_INFO);
        try {
            SearchRequest searchRequest = new SearchRequest().indices(configIndexName);
            if (rawPath.endsWith(RestHandlerUtils.COUNT)) {
                // Count detectors. With resource sharing the count covers only the configs visible to the caller.
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchRequest.source(searchSourceBuilder);
                pluginClient.search(searchRequest, new ActionListener<SearchResponse>() {

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
                matchName(searchRequest, listener);
            }
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    /**
     * Searches the config index for the requested name across all configs, not only those visible to the caller.
     * Create rejects a name that any config already uses, so the answer here must cover the same set. The plugin
     * client would scope the search to the caller when resource sharing is enabled, so this search runs in a
     * stashed context, as {@code AbstractTimeSeriesActionHandler#checkConfigNameExists} does. Keep the two on the
     * same path.
     */
    private void matchName(SearchRequest searchRequest, ActionListener<SearchConfigInfoResponse> listener) {
        ThreadContext threadContext = client.threadPool().getThreadContext();
        ActionListener<SearchConfigInfoResponse> callerListener = ContextPreservingActionListener
            .wrapPreservingContext(listener, threadContext);
        try (ThreadContext.StoredContext context = threadContext.stashContext()) {
            client.search(searchRequest, new ActionListener<SearchResponse>() {

                @Override
                public void onResponse(SearchResponse searchResponse) {
                    boolean nameExists = searchResponse.getHits().getTotalHits().value() > 0;
                    callerListener.onResponse(new SearchConfigInfoResponse(0, nameExists));
                }

                @Override
                public void onFailure(Exception e) {
                    if (e.getClass() == IndexNotFoundException.class) {
                        // Anomaly Detectors index does not exist
                        // Could be that user is creating first detector
                        callerListener.onResponse(new SearchConfigInfoResponse(0, false));
                    } else {
                        callerListener.onFailure(e);
                    }
                }
            });
        }
    }
}
