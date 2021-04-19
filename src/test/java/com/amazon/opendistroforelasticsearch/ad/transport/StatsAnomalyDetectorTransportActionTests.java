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
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.ad.transport;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import org.junit.Before;

import com.amazon.opendistroforelasticsearch.ad.ADIntegTestCase;
import com.amazon.opendistroforelasticsearch.ad.TestHelpers;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorType;
import com.amazon.opendistroforelasticsearch.ad.stats.InternalStatNames;
import com.amazon.opendistroforelasticsearch.ad.stats.StatNames;
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
                            AnomalyDetectorType.HISTORICAL_SINGLE_ENTITY.name(),
                            TestHelpers.randomDetectionDateRange(),
                            true
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

    public void testStatsAnomalyDetectorWithClusterLevelStats() throws IOException {
        ADStatsRequest adStatsRequest = new ADStatsRequest(clusterService().localNode());
        adStatsRequest.addStat(StatNames.DETECTOR_COUNT.getName());
        adStatsRequest.addStat(StatNames.HISTORICAL_SINGLE_ENTITY_DETECTOR_COUNT.getName());
        StatsAnomalyDetectorResponse response = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5_000);
        assertEquals(1, response.getAdStatsResponse().getADStatsNodesResponse().getNodes().size());
        Map<String, Object> statsMap = response.getAdStatsResponse().getADStatsNodesResponse().getNodes().get(0).getStatsMap();
        Map<String, Object> clusterStats = response.getAdStatsResponse().getClusterStats();
        assertEquals(0, statsMap.size());
        assertEquals(2L, clusterStats.get(StatNames.DETECTOR_COUNT.getName()));
        assertEquals(1L, clusterStats.get(StatNames.HISTORICAL_SINGLE_ENTITY_DETECTOR_COUNT.getName()));
    }

    public void testStatsAnomalyDetectorWithDetectorCount() throws IOException {
        ADStatsRequest adStatsRequest = new ADStatsRequest(clusterService().localNode());
        adStatsRequest.addStat(StatNames.DETECTOR_COUNT.getName());
        StatsAnomalyDetectorResponse response = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5_000);
        assertEquals(1, response.getAdStatsResponse().getADStatsNodesResponse().getNodes().size());
        Map<String, Object> statsMap = response.getAdStatsResponse().getADStatsNodesResponse().getNodes().get(0).getStatsMap();
        Map<String, Object> clusterStats = response.getAdStatsResponse().getClusterStats();
        assertEquals(0, statsMap.size());
        assertEquals(2L, clusterStats.get(StatNames.DETECTOR_COUNT.getName()));
        assertFalse(clusterStats.containsKey(StatNames.HISTORICAL_SINGLE_ENTITY_DETECTOR_COUNT.getName()));
    }

    public void testStatsAnomalyDetectorWithHistoricalDetectorCount() throws IOException {
        ADStatsRequest adStatsRequest = new ADStatsRequest(clusterService().localNode());
        adStatsRequest.addStat(StatNames.HISTORICAL_SINGLE_ENTITY_DETECTOR_COUNT.getName());
        StatsAnomalyDetectorResponse response = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5_000);
        assertEquals(1, response.getAdStatsResponse().getADStatsNodesResponse().getNodes().size());
        Map<String, Object> statsMap = response.getAdStatsResponse().getADStatsNodesResponse().getNodes().get(0).getStatsMap();
        Map<String, Object> clusterStats = response.getAdStatsResponse().getClusterStats();
        assertEquals(0, statsMap.size());
        assertEquals(1L, clusterStats.get(StatNames.HISTORICAL_SINGLE_ENTITY_DETECTOR_COUNT.getName()));
        assertFalse(clusterStats.containsKey(StatNames.DETECTOR_COUNT.getName()));
    }

}
