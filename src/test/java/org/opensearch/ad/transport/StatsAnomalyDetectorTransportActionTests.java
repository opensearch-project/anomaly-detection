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
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.stats.InternalStatNames;
import org.opensearch.timeseries.stats.StatNames;

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
        ADStatsRequest adStatsRequest = new ADStatsRequest(clusterService().localNode());
        adStatsRequest.addStat(InternalStatNames.JVM_HEAP_USAGE.getName());
        StatsAnomalyDetectorResponse response = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5_000);
        assertEquals(1, response.getAdStatsResponse().getADStatsNodesResponse().getNodes().size());
        assertTrue(
            response
                .getAdStatsResponse()
                .getADStatsNodesResponse()
                .getNodes()
                .get(0)
                .getStatsMap()
                .containsKey(InternalStatNames.JVM_HEAP_USAGE.getName())
        );
    }

    public void testStatsAnomalyDetectorWithClusterLevelStats() {
        ADStatsRequest adStatsRequest = new ADStatsRequest(clusterService().localNode());
        adStatsRequest.addStat(StatNames.DETECTOR_COUNT.getName());
        adStatsRequest.addStat(StatNames.SINGLE_ENTITY_DETECTOR_COUNT.getName());
        StatsAnomalyDetectorResponse response = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5_000);
        assertEquals(1, response.getAdStatsResponse().getADStatsNodesResponse().getNodes().size());
        Map<String, Object> statsMap = response.getAdStatsResponse().getADStatsNodesResponse().getNodes().get(0).getStatsMap();
        Map<String, Object> clusterStats = response.getAdStatsResponse().getClusterStats();
        assertEquals(0, statsMap.size());
        assertEquals(2L, clusterStats.get(StatNames.DETECTOR_COUNT.getName()));
        assertEquals(1L, clusterStats.get(StatNames.SINGLE_ENTITY_DETECTOR_COUNT.getName()));
    }

    public void testStatsAnomalyDetectorWithDetectorCount() {
        ADStatsRequest adStatsRequest = new ADStatsRequest(clusterService().localNode());
        adStatsRequest.addStat(StatNames.DETECTOR_COUNT.getName());
        StatsAnomalyDetectorResponse response = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5_000);
        assertEquals(1, response.getAdStatsResponse().getADStatsNodesResponse().getNodes().size());
        Map<String, Object> statsMap = response.getAdStatsResponse().getADStatsNodesResponse().getNodes().get(0).getStatsMap();
        Map<String, Object> clusterStats = response.getAdStatsResponse().getClusterStats();
        assertEquals(0, statsMap.size());
        assertEquals(2L, clusterStats.get(StatNames.DETECTOR_COUNT.getName()));
        assertFalse(clusterStats.containsKey(StatNames.SINGLE_ENTITY_DETECTOR_COUNT.getName()));
    }

    public void testStatsAnomalyDetectorWithSingleEntityDetectorCount() {
        ADStatsRequest adStatsRequest = new ADStatsRequest(clusterService().localNode());
        adStatsRequest.addStat(StatNames.SINGLE_ENTITY_DETECTOR_COUNT.getName());
        StatsAnomalyDetectorResponse response = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5_000);
        assertEquals(1, response.getAdStatsResponse().getADStatsNodesResponse().getNodes().size());
        Map<String, Object> statsMap = response.getAdStatsResponse().getADStatsNodesResponse().getNodes().get(0).getStatsMap();
        Map<String, Object> clusterStats = response.getAdStatsResponse().getClusterStats();
        assertEquals(0, statsMap.size());
        assertEquals(1L, clusterStats.get(StatNames.SINGLE_ENTITY_DETECTOR_COUNT.getName()));
        assertFalse(clusterStats.containsKey(StatNames.DETECTOR_COUNT.getName()));
    }

}
