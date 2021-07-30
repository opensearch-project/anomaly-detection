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

package org.opensearch.ad.transport;

import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_GET_DETECTOR_INFO;
import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.opensearch.ad.util.RestHandlerUtils.wrapRestActionListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class SearchAnomalyDetectorInfoTransportAction extends
    HandledTransportAction<SearchAnomalyDetectorInfoRequest, SearchAnomalyDetectorInfoResponse> {
    private static final Logger LOG = LogManager.getLogger(SearchAnomalyDetectorInfoTransportAction.class);
    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public SearchAnomalyDetectorInfoTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService
    ) {
        super(SearchAnomalyDetectorInfoAction.NAME, transportService, actionFilters, SearchAnomalyDetectorInfoRequest::new);
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(
        Task task,
        SearchAnomalyDetectorInfoRequest request,
        ActionListener<SearchAnomalyDetectorInfoResponse> actionListener
    ) {
        String name = request.getName();
        String rawPath = request.getRawPath();
        ActionListener<SearchAnomalyDetectorInfoResponse> listener = wrapRestActionListener(actionListener, FAIL_TO_GET_DETECTOR_INFO);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            SearchRequest searchRequest = new SearchRequest().indices(ANOMALY_DETECTORS_INDEX);
            if (rawPath.endsWith(RestHandlerUtils.COUNT)) {
                // Count detectors
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchRequest.source(searchSourceBuilder);
                client.search(searchRequest, new ActionListener<SearchResponse>() {

                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        SearchAnomalyDetectorInfoResponse response = new SearchAnomalyDetectorInfoResponse(
                            searchResponse.getHits().getTotalHits().value,
                            false
                        );
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e.getClass() == IndexNotFoundException.class) {
                            // Anomaly Detectors index does not exist
                            // Could be that user is creating first detector
                            SearchAnomalyDetectorInfoResponse response = new SearchAnomalyDetectorInfoResponse(0, false);
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
                        nameExists = searchResponse.getHits().getTotalHits().value > 0;
                        SearchAnomalyDetectorInfoResponse response = new SearchAnomalyDetectorInfoResponse(0, nameExists);
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e.getClass() == IndexNotFoundException.class) {
                            // Anomaly Detectors index does not exist
                            // Could be that user is creating first detector
                            SearchAnomalyDetectorInfoResponse response = new SearchAnomalyDetectorInfoResponse(0, false);
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
