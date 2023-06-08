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

import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_GET_STATS;
import static org.opensearch.ad.util.RestHandlerUtils.wrapRestActionListener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorType;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.stats.ADStatsResponse;
import org.opensearch.ad.stats.StatNames;
import org.opensearch.ad.util.MultiResponsesDelegateActionListener;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;

import com.google.inject.Inject;

public class StatsAnomalyDetectorTransportAction extends TransportAction<ADStatsRequest, StatsAnomalyDetectorResponse> {
    public static final String DETECTOR_TYPE_AGG = "detector_type_agg";
    private final Logger logger = LogManager.getLogger(StatsAnomalyDetectorTransportAction.class);

    private final SDKRestClient sdkRestClient;
    private final ADStats adStats;
    private final SDKClusterService sdkClusterService;

    @Inject
    public StatsAnomalyDetectorTransportAction(
        ActionFilters actionFilters,
        SDKRestClient sdkRestClient,
        ADStats adStats,
        SDKClusterService sdkClusterService,
        TaskManager taskManager

    ) {
        super(StatsAnomalyDetectorAction.NAME, actionFilters, taskManager);
        this.sdkRestClient = sdkRestClient;
        this.adStats = adStats;
        this.sdkClusterService = sdkClusterService;
    }

    @Override
    protected void doExecute(Task task, ADStatsRequest request, ActionListener<StatsAnomalyDetectorResponse> actionListener) {
        ActionListener<StatsAnomalyDetectorResponse> listener = wrapRestActionListener(actionListener, FAIL_TO_GET_STATS);
        try {
            getStats(sdkRestClient, listener, request);
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    /**
     * Make the 2 requests to get the node and cluster statistics
     *
     * @param sdkRestClient SDKRestClient
     * @param listener Listener to send response
     * @param adStatsRequest Request containing stats to be retrieved
     */
    private void getStats(
        SDKRestClient sdkRestClient,
        ActionListener<StatsAnomalyDetectorResponse> listener,
        ADStatsRequest adStatsRequest
    ) {
        // Use MultiResponsesDelegateActionListener to execute 2 async requests and create the response once they finish
        MultiResponsesDelegateActionListener<ADStatsResponse> delegateListener = new MultiResponsesDelegateActionListener<>(
            getRestStatsListener(listener),
            2,
            "Unable to return AD Stats",
            false
        );

        getClusterStats(sdkRestClient, delegateListener, adStatsRequest);
        getNodeStats(sdkRestClient, delegateListener, adStatsRequest);
    }

    /**
     * Listener sends response once Node Stats and Cluster Stats are gathered
     *
     * @param listener Listener to send response
     * @return ActionListener for ADStatsResponse
     */
    private ActionListener<ADStatsResponse> getRestStatsListener(ActionListener<StatsAnomalyDetectorResponse> listener) {
        return ActionListener
            .wrap(
                adStatsResponse -> { listener.onResponse(new StatsAnomalyDetectorResponse(adStatsResponse)); },
                exception -> listener.onFailure(new OpenSearchStatusException(exception.getMessage(), RestStatus.INTERNAL_SERVER_ERROR))
            );
    }

    /**
     * Make async request to get the number of detectors in AnomalyDetector.ANOMALY_DETECTORS_INDEX if necessary
     * and, onResponse, gather the cluster statistics
     *
     * @param sdkRestClient SDKRestClient
     * @param listener MultiResponsesDelegateActionListener to be used once both requests complete
     * @param adStatsRequest Request containing stats to be retrieved
     */
    private void getClusterStats(
        SDKRestClient sdkRestClient,
        MultiResponsesDelegateActionListener<ADStatsResponse> listener,
        ADStatsRequest adStatsRequest
    ) {
        ADStatsResponse adStatsResponse = new ADStatsResponse();
        if ((adStatsRequest.getStatsToBeRetrieved().contains(StatNames.DETECTOR_COUNT.getName())
            || adStatsRequest.getStatsToBeRetrieved().contains(StatNames.SINGLE_ENTITY_DETECTOR_COUNT.getName())
            || adStatsRequest.getStatsToBeRetrieved().contains(StatNames.MULTI_ENTITY_DETECTOR_COUNT.getName()))
            && anomalyDetectorsIndexExists()) {

            TermsAggregationBuilder termsAgg = AggregationBuilders.terms(DETECTOR_TYPE_AGG).field(AnomalyDetector.DETECTOR_TYPE_FIELD);
            SearchRequest request = new SearchRequest()
                .indices(AnomalyDetector.ANOMALY_DETECTORS_INDEX)
                .source(new SearchSourceBuilder().aggregation(termsAgg).size(0).trackTotalHits(true));

            sdkRestClient.search(request, ActionListener.wrap(r -> {
                ParsedStringTerms aggregation = r.getAggregations().get(DETECTOR_TYPE_AGG);
                List<ParsedStringTerms.ParsedBucket> buckets = (List<ParsedStringTerms.ParsedBucket>) aggregation.getBuckets();
                long totalDetectors = r.getHits().getTotalHits().value;
                long totalSingleEntityDetectors = 0;
                long totalMultiEntityDetectors = 0;
                for (ParsedStringTerms.ParsedBucket b : buckets) {
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
                    adStats.getStat(StatNames.DETECTOR_COUNT.getName()).setValue(totalDetectors);
                }
                if (adStatsRequest.getStatsToBeRetrieved().contains(StatNames.SINGLE_ENTITY_DETECTOR_COUNT.getName())) {
                    adStats.getStat(StatNames.SINGLE_ENTITY_DETECTOR_COUNT.getName()).setValue(totalSingleEntityDetectors);
                }
                if (adStatsRequest.getStatsToBeRetrieved().contains(StatNames.MULTI_ENTITY_DETECTOR_COUNT.getName())) {
                    adStats.getStat(StatNames.MULTI_ENTITY_DETECTOR_COUNT.getName()).setValue(totalMultiEntityDetectors);
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
     * Collect Cluster Stats into map to be retrieved
     *
     * @param adStatsRequest Request containing stats to be retrieved
     * @return Map containing Cluster Stats
     */
    private Map<String, Object> getClusterStatsMap(ADStatsRequest adStatsRequest) {
        Map<String, Object> clusterStats = new HashMap<>();
        Set<String> statsToBeRetrieved = adStatsRequest.getStatsToBeRetrieved();
        adStats
            .getClusterStats()
            .entrySet()
            .stream()
            .filter(s -> statsToBeRetrieved.contains(s.getKey()))
            .forEach(s -> clusterStats.put(s.getKey(), s.getValue().getValue()));
        return clusterStats;
    }

    /**
     * Make async request to get the Anomaly Detection statistics from each node and, onResponse, set the
     * ADStatsNodesResponse field of ADStatsResponse
     *
     * @param sdkRestClient SDKRestClient
     * @param listener MultiResponsesDelegateActionListener to be used once both requests complete
     * @param adStatsRequest Request containing stats to be retrieved
     */
    private void getNodeStats(
        SDKRestClient sdkRestClient,
        MultiResponsesDelegateActionListener<ADStatsResponse> listener,
        ADStatsRequest adStatsRequest
    ) {
        sdkRestClient.execute(ADStatsNodesAction.INSTANCE, adStatsRequest, ActionListener.wrap(adStatsResponse -> {
            ADStatsResponse restADStatsResponse = new ADStatsResponse();
            restADStatsResponse.setADStatsNodesResponse(adStatsResponse);
            listener.onResponse(restADStatsResponse);
        }, listener::onFailure));
    }

    private boolean anomalyDetectorsIndexExists() {
        GetIndexRequest getindexRequest = new GetIndexRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX);

        CompletableFuture<Boolean> existsFuture = new CompletableFuture<>();
        sdkRestClient.indices().exists(getindexRequest, ActionListener.wrap(response -> { existsFuture.complete(response); }, exception -> {
            existsFuture.completeExceptionally(exception);
        }));

        Boolean existsResponse = existsFuture
            .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(Settings.EMPTY).getMillis(), TimeUnit.MILLISECONDS)
            .join();

        return existsResponse.booleanValue();
    }
}
