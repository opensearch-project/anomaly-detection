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

import java.util.List;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorType;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.transport.BaseStatsTransportAction;
import org.opensearch.timeseries.transport.StatsRequest;
import org.opensearch.timeseries.transport.StatsResponse;
import org.opensearch.timeseries.util.MultiResponsesDelegateActionListener;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class StatsAnomalyDetectorTransportAction extends BaseStatsTransportAction {
    public static final String DETECTOR_TYPE_AGG = "detector_type_agg";

    @Inject
    public StatsAnomalyDetectorTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ADStats adStats,
        ClusterService clusterService

    ) {
        super(transportService, actionFilters, client, adStats, clusterService, StatsAnomalyDetectorAction.NAME);
    }

    /**
     * Make async request to get the number of detectors in AnomalyDetector.ANOMALY_DETECTORS_INDEX if necessary
     * and, onResponse, gather the cluster statistics
     *
     * @param client Client
     * @param listener MultiResponsesDelegateActionListener to be used once both requests complete
     * @param adStatsRequest Request containing stats to be retrieved
     */
    @Override
    protected void getClusterStats(
        Client client,
        MultiResponsesDelegateActionListener<StatsResponse> listener,
        StatsRequest adStatsRequest
    ) {
        StatsResponse adStatsResponse = new StatsResponse();
        if ((adStatsRequest.getStatsToBeRetrieved().contains(StatNames.DETECTOR_COUNT.getName())
            || adStatsRequest.getStatsToBeRetrieved().contains(StatNames.SINGLE_STREAM_DETECTOR_COUNT.getName())
            || adStatsRequest.getStatsToBeRetrieved().contains(StatNames.HC_DETECTOR_COUNT.getName()))
            && clusterService.state().getRoutingTable().hasIndex(CommonName.CONFIG_INDEX)) {

            TermsAggregationBuilder termsAgg = AggregationBuilders.terms(DETECTOR_TYPE_AGG).field(AnomalyDetector.DETECTOR_TYPE_FIELD);
            SearchRequest request = new SearchRequest()
                .indices(CommonName.CONFIG_INDEX)
                .source(new SearchSourceBuilder().aggregation(termsAgg).size(0).trackTotalHits(true));

            client.search(request, ActionListener.wrap(r -> {
                StringTerms aggregation = r.getAggregations().get(DETECTOR_TYPE_AGG);
                List<StringTerms.Bucket> buckets = aggregation.getBuckets();
                long totalDetectors = r.getHits().getTotalHits().value();
                long totalSingleEntityDetectors = 0;
                long totalMultiEntityDetectors = 0;
                for (StringTerms.Bucket b : buckets) {
                    if (AnomalyDetectorType.SINGLE_ENTITY.name().equals(b.getKeyAsString())
                        || AnomalyDetectorType.REALTIME_SINGLE_ENTITY.name().equals(b.getKeyAsString())
                        || AnomalyDetectorType.HISTORICAL_SINGLE_ENTITY.name().equals(b.getKeyAsString())) {
                        totalSingleEntityDetectors += b.getDocCount();
                    }
                    if (AnomalyDetectorType.MULTI_ENTITY.name().equals(b.getKeyAsString())
                        || AnomalyDetectorType.REALTIME_MULTI_ENTITY.name().equals(b.getKeyAsString())
                        || AnomalyDetectorType.HISTORICAL_MULTI_ENTITY.name().equals(b.getKeyAsString())) {
                        totalMultiEntityDetectors += b.getDocCount();
                    }
                }
                if (adStatsRequest.getStatsToBeRetrieved().contains(StatNames.DETECTOR_COUNT.getName())) {
                    stats.getStat(StatNames.DETECTOR_COUNT.getName()).setValue(totalDetectors);
                }
                if (adStatsRequest.getStatsToBeRetrieved().contains(StatNames.SINGLE_STREAM_DETECTOR_COUNT.getName())) {
                    stats.getStat(StatNames.SINGLE_STREAM_DETECTOR_COUNT.getName()).setValue(totalSingleEntityDetectors);
                }
                if (adStatsRequest.getStatsToBeRetrieved().contains(StatNames.HC_DETECTOR_COUNT.getName())) {
                    stats.getStat(StatNames.HC_DETECTOR_COUNT.getName()).setValue(totalMultiEntityDetectors);
                }
                adStatsResponse.setClusterStats(getClusterStatsMap(adStatsRequest));
                listener.onResponse(adStatsResponse);
            }, e -> listener.onFailure(e)));
        } else {
            adStatsResponse.setClusterStats(getClusterStatsMap(adStatsRequest));
            listener.onResponse(adStatsResponse);
        }
    }

    /**
     * Make async request to get the Anomaly Detection statistics from each node and, onResponse, set the
     * ADStatsNodesResponse field of ADStatsResponse
     *
     * @param client Client
     * @param listener MultiResponsesDelegateActionListener to be used once both requests complete
     * @param adStatsRequest Request containing stats to be retrieved
     */
    @Override
    protected void getNodeStats(Client client, MultiResponsesDelegateActionListener<StatsResponse> listener, StatsRequest adStatsRequest) {
        client.execute(ADStatsNodesAction.INSTANCE, adStatsRequest, ActionListener.wrap(adStatsResponse -> {
            StatsResponse restADStatsResponse = new StatsResponse();
            restADStatsResponse.setStatsNodesResponse(adStatsResponse);
            listener.onResponse(restADStatsResponse);
        }, listener::onFailure));
    }
}
