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

import java.time.Instant;
import java.util.Map;

import org.junit.Before;
import org.opensearch.ad.ADIntegTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.stats.InternalStatNames;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.transport.StatsRequest;
import org.opensearch.timeseries.transport.StatsTimeSeriesResponse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class StatsAnomalyDetectorTransportActionTests extends ADIntegTestCase {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        createDetectors(
            ImmutableList
                .of(
                    TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now()),
                    TestHelpers
                        .randomAnomalyDetector(
                            ImmutableList.of(TestHelpers.randomFeature()),
                            ImmutableMap.of(),
                            Instant.now(),
                            true,
                            ImmutableList.of(randomAlphaOfLength(5))
                        )
                ),
            true
        );
    }

    public void testStatsAnomalyDetectorWithNodeLevelStats() {
        StatsRequest adStatsRequest = new StatsRequest(clusterService().localNode());
        adStatsRequest.addStat(InternalStatNames.JVM_HEAP_USAGE.getName());
        StatsTimeSeriesResponse response = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5_000);
        assertEquals(1, response.getAdStatsResponse().getStatsNodesResponse().getNodes().size());
        assertTrue(
            response
                .getAdStatsResponse()
                .getStatsNodesResponse()
                .getNodes()
                .get(0)
                .getStatsMap()
                .containsKey(InternalStatNames.JVM_HEAP_USAGE.getName())
        );
    }

    public void testStatsAnomalyDetectorWithClusterLevelStats() {
        StatsRequest adStatsRequest = new StatsRequest(clusterService().localNode());
        adStatsRequest.addStat(StatNames.DETECTOR_COUNT.getName());
        adStatsRequest.addStat(StatNames.SINGLE_STREAM_DETECTOR_COUNT.getName());
        StatsTimeSeriesResponse response = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5_000);
        assertEquals(1, response.getAdStatsResponse().getStatsNodesResponse().getNodes().size());
        Map<String, Object> statsMap = response.getAdStatsResponse().getStatsNodesResponse().getNodes().get(0).getStatsMap();
        Map<String, Object> clusterStats = response.getAdStatsResponse().getClusterStats();
        assertEquals(0, statsMap.size());
        assertEquals(2L, clusterStats.get(StatNames.DETECTOR_COUNT.getName()));
        assertEquals(1L, clusterStats.get(StatNames.SINGLE_STREAM_DETECTOR_COUNT.getName()));
    }

    public void testStatsAnomalyDetectorWithDetectorCount() {
        StatsRequest adStatsRequest = new StatsRequest(clusterService().localNode());
        adStatsRequest.addStat(StatNames.DETECTOR_COUNT.getName());
        StatsTimeSeriesResponse response = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5_000);
        assertEquals(1, response.getAdStatsResponse().getStatsNodesResponse().getNodes().size());
        Map<String, Object> statsMap = response.getAdStatsResponse().getStatsNodesResponse().getNodes().get(0).getStatsMap();
        Map<String, Object> clusterStats = response.getAdStatsResponse().getClusterStats();
        assertEquals(0, statsMap.size());
        assertEquals(2L, clusterStats.get(StatNames.DETECTOR_COUNT.getName()));
        assertFalse(clusterStats.containsKey(StatNames.SINGLE_STREAM_DETECTOR_COUNT.getName()));
    }

    public void testStatsAnomalyDetectorWithSingleEntityDetectorCount() {
        StatsRequest adStatsRequest = new StatsRequest(clusterService().localNode());
        adStatsRequest.addStat(StatNames.SINGLE_STREAM_DETECTOR_COUNT.getName());
        StatsTimeSeriesResponse response = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5_000);
        assertEquals(1, response.getAdStatsResponse().getStatsNodesResponse().getNodes().size());
        Map<String, Object> statsMap = response.getAdStatsResponse().getStatsNodesResponse().getNodes().get(0).getStatsMap();
        Map<String, Object> clusterStats = response.getAdStatsResponse().getClusterStats();
        assertEquals(0, statsMap.size());
        assertEquals(1L, clusterStats.get(StatNames.SINGLE_STREAM_DETECTOR_COUNT.getName()));
        assertFalse(clusterStats.containsKey(StatNames.DETECTOR_COUNT.getName()));
    }

}
