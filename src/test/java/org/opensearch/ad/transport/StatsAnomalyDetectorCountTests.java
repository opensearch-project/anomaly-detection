/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetectorType;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.TimeSeriesStat;
import org.opensearch.timeseries.stats.suppliers.SettableSupplier;
import org.opensearch.timeseries.transport.StatsRequest;
import org.opensearch.timeseries.transport.StatsResponse;
import org.opensearch.timeseries.util.MultiResponsesDelegateActionListener;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/** Tests that each stats request reports the detector counts its own search found. */
public class StatsAnomalyDetectorCountTests extends OpenSearchTestCase {

    private final List<ActionListener<SearchResponse>> pendingSearches = new ArrayList<>();
    private final AtomicReference<Runnable> onCountWritten = new AtomicReference<>();
    private Map<String, TimeSeriesStat<?>> statsMap;
    private StatsAnomalyDetectorTransportAction action;
    private Client client;
    private ClusterService clusterService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        statsMap = new HashMap<>();
        statsMap.put(StatNames.DETECTOR_COUNT.getName(), new TimeSeriesStat<>(true, new SettableSupplier()));
        statsMap.put(StatNames.SINGLE_STREAM_DETECTOR_COUNT.getName(), new TimeSeriesStat<>(true, new SettableSupplier()));
        // Stands in for a second thread: a write to the shared count completes the other pending request.
        statsMap.put(StatNames.HC_DETECTOR_COUNT.getName(), new TimeSeriesStat<>(true, new SettableSupplier() {
            @Override
            public void set(Long value) {
                super.set(value);
                Runnable hook = onCountWritten.getAndSet(null);
                if (hook != null) {
                    hook.run();
                }
            }
        }));

        statsMap.put(StatNames.AD_CONFIG_INDEX_STATUS.getName(), new TimeSeriesStat<>(true, () -> "green"));

        client = mock(Client.class);
        doAnswer(invocation -> {
            pendingSearches.add(invocation.getArgument(1));
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        when(clusterService.state().getRoutingTable().hasIndex(ADCommonName.CONFIG_INDEX)).thenReturn(true);

        action = new StatsAnomalyDetectorTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client,
            null,
            new ADStats(statsMap),
            clusterService
        );
    }

    public void testConcurrentRequestsEachReportTheirOwnCount() {
        AtomicReference<StatsResponse> first = startCountRequest();
        AtomicReference<StatsResponse> second = startCountRequest();
        assertEquals(2, pendingSearches.size());

        onCountWritten.set(() -> pendingSearches.get(1).onResponse(countResponse(5, 2)));
        pendingSearches.get(0).onResponse(countResponse(1, 0));
        if (onCountWritten.get() != null) {
            pendingSearches.get(1).onResponse(countResponse(5, 2));
        }

        assertEquals(1L, first.get().getClusterStats().get(StatNames.HC_DETECTOR_COUNT.getName()));
        assertEquals(1L, first.get().getClusterStats().get(StatNames.DETECTOR_COUNT.getName()));
        assertEquals(5L, second.get().getClusterStats().get(StatNames.HC_DETECTOR_COUNT.getName()));
        assertEquals(2L, second.get().getClusterStats().get(StatNames.SINGLE_STREAM_DETECTOR_COUNT.getName()));
        assertEquals(7L, second.get().getClusterStats().get(StatNames.DETECTOR_COUNT.getName()));
    }

    public void testCountIsNotWrittenToSharedStats() {
        AtomicReference<StatsResponse> response = startCountRequest();
        pendingSearches.get(0).onResponse(countResponse(3, 4));

        assertEquals(3L, response.get().getClusterStats().get(StatNames.HC_DETECTOR_COUNT.getName()));
        for (StatNames stat : List.of(StatNames.DETECTOR_COUNT, StatNames.SINGLE_STREAM_DETECTOR_COUNT, StatNames.HC_DETECTOR_COUNT)) {
            assertEquals(stat.getName(), 0L, statsMap.get(stat.getName()).getValue());
        }
    }

    public void testOnlyRequestedCountsAreReportedNextToOtherClusterStats() {
        AtomicReference<StatsResponse> response = startRequest(StatNames.HC_DETECTOR_COUNT, StatNames.AD_CONFIG_INDEX_STATUS);
        pendingSearches.get(0).onResponse(countResponse(3, 4));

        Map<String, Object> clusterStats = response.get().getClusterStats();
        assertEquals(3L, clusterStats.get(StatNames.HC_DETECTOR_COUNT.getName()));
        assertEquals("green", clusterStats.get(StatNames.AD_CONFIG_INDEX_STATUS.getName()));
        assertFalse(clusterStats.containsKey(StatNames.DETECTOR_COUNT.getName()));
        assertFalse(clusterStats.containsKey(StatNames.SINGLE_STREAM_DETECTOR_COUNT.getName()));
    }

    public void testCountsAreZeroWithoutAConfigIndex() {
        when(clusterService.state().getRoutingTable().hasIndex(ADCommonName.CONFIG_INDEX)).thenReturn(false);
        AtomicReference<StatsResponse> response = startCountRequest();

        assertTrue(pendingSearches.isEmpty());
        for (StatNames stat : List.of(StatNames.DETECTOR_COUNT, StatNames.SINGLE_STREAM_DETECTOR_COUNT, StatNames.HC_DETECTOR_COUNT)) {
            assertEquals(stat.getName(), 0L, response.get().getClusterStats().get(stat.getName()));
        }
    }

    private AtomicReference<StatsResponse> startCountRequest() {
        return startRequest(StatNames.DETECTOR_COUNT, StatNames.SINGLE_STREAM_DETECTOR_COUNT, StatNames.HC_DETECTOR_COUNT);
    }

    private AtomicReference<StatsResponse> startRequest(StatNames... stats) {
        StatsRequest request = new StatsRequest(new String[0]);
        for (StatNames stat : stats) {
            request.addStat(stat.getName());
        }
        AtomicReference<StatsResponse> result = new AtomicReference<>();
        action
            .getClusterStats(
                client,
                new MultiResponsesDelegateActionListener<>(
                    ActionListener.wrap(result::set, e -> fail(e.getMessage())),
                    1,
                    "Unable to return Stats",
                    false
                ),
                request
            );
        return result;
    }

    private static SearchResponse countResponse(long hcDetectors, long singleStreamDetectors) {
        Terms terms = mock(Terms.class);
        when(terms.getName()).thenReturn(StatsAnomalyDetectorTransportAction.DETECTOR_TYPE_AGG);
        doReturn(
            List.of(bucket(AnomalyDetectorType.MULTI_ENTITY, hcDetectors), bucket(AnomalyDetectorType.SINGLE_ENTITY, singleStreamDetectors))
        ).when(terms).getBuckets();
        SearchResponse response = mock(SearchResponse.class);
        when(response.getAggregations()).thenReturn(new Aggregations(List.of(terms)));
        TotalHits totalHits = new TotalHits(hcDetectors + singleStreamDetectors, TotalHits.Relation.EQUAL_TO);
        when(response.getHits()).thenReturn(new SearchHits(new SearchHit[0], totalHits, 0));
        return response;
    }

    private static Terms.Bucket bucket(AnomalyDetectorType type, long count) {
        Terms.Bucket bucket = mock(Terms.Bucket.class);
        when(bucket.getKeyAsString()).thenReturn(type.name());
        when(bucket.getDocCount()).thenReturn(count);
        return bucket;
    }
}
