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

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.rest.handler.InsightsJobActionHandler;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.transport.InsightsJobRequest;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class InsightsJobTransportAction extends HandledTransportAction<InsightsJobRequest, InsightsJobResponse> {
    private static final Logger log = LogManager.getLogger(InsightsJobTransportAction.class);

    private final Client client;
    private final InsightsJobActionHandler jobHandler;

    @Inject
    public InsightsJobTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        ADIndexManagement indexManagement
    ) {
        super(InsightsJobAction.NAME, transportService, actionFilters, InsightsJobRequest::new);
        this.client = client;
        this.jobHandler = new InsightsJobActionHandler(
            client,
            xContentRegistry,
            indexManagement,
            AnomalyDetectorSettings.AD_REQUEST_TIMEOUT.get(settings)
        );
    }

    @Override
    protected void doExecute(Task task, InsightsJobRequest request, ActionListener<InsightsJobResponse> listener) {
        if (request.isStartOperation()) {
            handleStartOperation(request, listener);
        } else if (request.isStatusOperation()) {
            handleStatusOperation(request, listener);
        } else if (request.isStopOperation()) {
            handleStopOperation(request, listener);
        } else if (request.isResultsOperation()) {
            handleResultsOperation(request, listener);
        } else {
            listener.onFailure(new IllegalArgumentException("Unknown operation"));
        }
    }

    private void handleStartOperation(InsightsJobRequest request, ActionListener<InsightsJobResponse> listener) {
        log.info("Starting insights job with frequency: {}", request.getFrequency());

        jobHandler.startInsightsJob(request.getFrequency(), listener);
    }

    private void handleStatusOperation(InsightsJobRequest request, ActionListener<InsightsJobResponse> listener) {
        jobHandler.getInsightsJobStatus(listener);
    }

    private void handleStopOperation(InsightsJobRequest request, ActionListener<InsightsJobResponse> listener) {
        jobHandler.stopInsightsJob(listener);
    }

    private void handleResultsOperation(InsightsJobRequest request, ActionListener<InsightsJobResponse> listener) {
        try {
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

            if (request.getDetectorId() != null && !request.getDetectorId().isEmpty()) {
                boolQuery.must(QueryBuilders.termQuery("doc_detector_ids", request.getDetectorId()));
            }

            if (request.getIndex() != null && !request.getIndex().isEmpty()) {
                boolQuery.must(QueryBuilders.termQuery("doc_indices", request.getIndex()));
            }

            if (!boolQuery.hasClauses()) {
                boolQuery.must(QueryBuilders.matchAllQuery());
            }

            SearchRequest searchRequest = new SearchRequest(ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS)
                .source(
                    new SearchSourceBuilder()
                        .query(boolQuery)
                        .from(request.getFrom())
                        .size(request.getSize())
                        .sort("generated_at", SortOrder.DESC)
                );

            client.search(searchRequest, ActionListener.wrap(searchResponse -> {
                long totalHits = searchResponse.getHits().getTotalHits() != null ? searchResponse.getHits().getTotalHits().value() : 0;
                log.debug("Search completed, found {} hits", totalHits);
                handleSearchResponse(searchResponse, listener);
            }, e -> {
                if (e.getMessage() != null && e.getMessage().contains("No mapping found")) {
                    listener.onResponse(new InsightsJobResponse(new ArrayList<>(), 0L));
                } else {
                    log.error("Failed to search insights results", e);
                    listener.onFailure(e);
                }
            }));

        } catch (Exception e) {
            log.error("Error building search request for insights results", e);
            listener.onFailure(e);
        }
    }

    private void handleSearchResponse(SearchResponse searchResponse, ActionListener<InsightsJobResponse> listener) {
        try {
            List<String> results = new ArrayList<>();

            for (SearchHit hit : searchResponse.getHits().getHits()) {
                results.add(hit.getSourceAsString());
            }

            long totalHits = searchResponse.getHits().getTotalHits() != null ? searchResponse.getHits().getTotalHits().value() : 0;
            listener.onResponse(new InsightsJobResponse(results, totalHits));
        } catch (Exception e) {
            log.error("Error processing search response", e);
            listener.onFailure(e);
        }
    }
}
