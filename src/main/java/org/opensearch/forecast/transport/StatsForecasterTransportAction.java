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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.stats.ForecastStats;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.SingleBucketAggregation;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.transport.BaseStatsTransportAction;
import org.opensearch.timeseries.transport.StatsRequest;
import org.opensearch.timeseries.transport.StatsResponse;
import org.opensearch.timeseries.util.MultiResponsesDelegateActionListener;
import org.opensearch.transport.TransportService;

public class StatsForecasterTransportAction extends BaseStatsTransportAction {
    public final Logger logger = LogManager.getLogger(StatsForecasterTransportAction.class);
    private final String WITH_CATEGORY_FIELD = "with_category_field";
    private final String WITHOUT_CATEGORY_FIELD = "without_category_field";

    @Inject
    public StatsForecasterTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ForecastStats stats,
        ClusterService clusterService

    ) {
        super(transportService, actionFilters, client, stats, clusterService, StatsForecasterAction.NAME);
    }

    /**
     * Make async request to get the number of detectors in AnomalyDetector.ANOMALY_DETECTORS_INDEX if necessary
     * and, onResponse, gather the cluster statistics
     *
     * @param client Client
     * @param listener MultiResponsesDelegateActionListener to be used once both requests complete
     * @param statsRequest Request containing stats to be retrieved
     */
    @Override
    public void getClusterStats(Client client, MultiResponsesDelegateActionListener<StatsResponse> listener, StatsRequest statsRequest) {
        StatsResponse adStatsResponse = new StatsResponse();
        if ((statsRequest.getStatsToBeRetrieved().contains(StatNames.FORECASTER_COUNT.getName())
            || statsRequest.getStatsToBeRetrieved().contains(StatNames.SINGLE_STREAM_FORECASTER_COUNT.getName())
            || statsRequest.getStatsToBeRetrieved().contains(StatNames.HC_FORECASTER_COUNT.getName()))
            && clusterService.state().getRoutingTable().hasIndex(ForecastCommonName.CONFIG_INDEX)) {

            // Create the query
            ExistsQueryBuilder existsQuery = QueryBuilders.existsQuery(Config.CATEGORY_FIELD);
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().mustNot(existsQuery);

            FilterAggregationBuilder withFieldAgg = AggregationBuilders.filter(WITH_CATEGORY_FIELD, existsQuery);
            FilterAggregationBuilder withoutFieldAgg = AggregationBuilders.filter(WITHOUT_CATEGORY_FIELD, boolQuery);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchSourceBuilder.size(0);
            searchSourceBuilder.aggregation(withFieldAgg);
            searchSourceBuilder.aggregation(withoutFieldAgg);

            SearchRequest searchRequest = new SearchRequest(ForecastCommonName.CONFIG_INDEX);
            searchRequest.source(searchSourceBuilder);

            // Execute the query
            client.search(searchRequest, ActionListener.wrap(searchResponse -> {
                // Parse the response
                SingleBucketAggregation withField = (SingleBucketAggregation) searchResponse.getAggregations().get(WITH_CATEGORY_FIELD);
                SingleBucketAggregation withoutField = (SingleBucketAggregation) searchResponse
                    .getAggregations()
                    .get(WITHOUT_CATEGORY_FIELD);
                if (statsRequest.getStatsToBeRetrieved().contains(StatNames.FORECASTER_COUNT.getName())) {
                    stats.getStat(StatNames.FORECASTER_COUNT.getName()).setValue(withField.getDocCount() + withoutField.getDocCount());
                }
                if (statsRequest.getStatsToBeRetrieved().contains(StatNames.SINGLE_STREAM_FORECASTER_COUNT.getName())) {
                    stats.getStat(StatNames.SINGLE_STREAM_FORECASTER_COUNT.getName()).setValue(withoutField.getDocCount());
                }
                if (statsRequest.getStatsToBeRetrieved().contains(StatNames.HC_FORECASTER_COUNT.getName())) {
                    stats.getStat(StatNames.HC_FORECASTER_COUNT.getName()).setValue(withField.getDocCount());
                }
                adStatsResponse.setClusterStats(getClusterStatsMap(statsRequest));
                listener.onResponse(adStatsResponse);
            }, e -> listener.onFailure(e)));
        } else {
            adStatsResponse.setClusterStats(getClusterStatsMap(statsRequest));
            listener.onResponse(adStatsResponse);
        }
    }

    /**
     * Make async request to get the forecasting statistics from each node and, onResponse, set the
     * StatsNodesResponse field of StatsResponse
     *
     * @param client Client
     * @param listener MultiResponsesDelegateActionListener to be used once both requests complete
     * @param statsRequest Request containing stats to be retrieved
     */
    @Override
    public void getNodeStats(Client client, MultiResponsesDelegateActionListener<StatsResponse> listener, StatsRequest statsRequest) {
        client.execute(ForecastStatsNodesAction.INSTANCE, statsRequest, ActionListener.wrap(adStatsResponse -> {
            StatsResponse restStatsResponse = new StatsResponse();
            restStatsResponse.setStatsNodesResponse(adStatsResponse);
            listener.onResponse(restStatsResponse);
        }, listener::onFailure));
    }
}
