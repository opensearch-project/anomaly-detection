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

package org.opensearch.ad.stats;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.transport.StatsNodeResponse;
import org.opensearch.timeseries.transport.StatsNodesResponse;
import org.opensearch.timeseries.transport.StatsResponse;

public class ADStatsResponseTests extends OpenSearchTestCase {
    @Test
    public void testGetAndSetClusterStats() {
        StatsResponse adStatsResponse = new StatsResponse();
        Map<String, Object> testClusterStats = new HashMap<>();
        testClusterStats.put("test_stat", 1L);
        adStatsResponse.setClusterStats(testClusterStats);
        assertEquals(testClusterStats, adStatsResponse.getClusterStats());
    }

    @Test
    public void testGetAndSetADStatsNodesResponse() {
        StatsResponse adStatsResponse = new StatsResponse();
        List<StatsNodeResponse> responses = Collections.emptyList();
        List<FailedNodeException> failures = Collections.emptyList();
        StatsNodesResponse adStatsNodesResponse = new StatsNodesResponse(ClusterName.DEFAULT, responses, failures);
        adStatsResponse.setStatsNodesResponse(adStatsNodesResponse);
        assertEquals(adStatsNodesResponse, adStatsResponse.getStatsNodesResponse());
    }

    @Test
    public void testMerge() {
        StatsResponse adStatsResponse1 = new StatsResponse();
        Map<String, Object> testClusterStats = new HashMap<>();
        testClusterStats.put("test_stat", 1L);
        adStatsResponse1.setClusterStats(testClusterStats);

        StatsResponse adStatsResponse2 = new StatsResponse();
        List<StatsNodeResponse> responses = Collections.emptyList();
        List<FailedNodeException> failures = Collections.emptyList();
        StatsNodesResponse adStatsNodesResponse = new StatsNodesResponse(ClusterName.DEFAULT, responses, failures);
        adStatsResponse2.setStatsNodesResponse(adStatsNodesResponse);

        adStatsResponse1.merge(adStatsResponse2);
        assertEquals(testClusterStats, adStatsResponse1.getClusterStats());
        assertEquals(adStatsNodesResponse, adStatsResponse1.getStatsNodesResponse());

        adStatsResponse2.merge(adStatsResponse1);
        assertEquals(testClusterStats, adStatsResponse2.getClusterStats());
        assertEquals(adStatsNodesResponse, adStatsResponse2.getStatsNodesResponse());

        // Confirm merging with null does nothing
        adStatsResponse1.merge(null);
        assertEquals(testClusterStats, adStatsResponse1.getClusterStats());
        assertEquals(adStatsNodesResponse, adStatsResponse1.getStatsNodesResponse());

        // Confirm merging with self does nothing
        adStatsResponse1.merge(adStatsResponse1);
        assertEquals(testClusterStats, adStatsResponse1.getClusterStats());
        assertEquals(adStatsNodesResponse, adStatsResponse1.getStatsNodesResponse());
    }

    @Test
    public void testEquals() {
        StatsResponse adStatsResponse1 = new StatsResponse();
        assertEquals(adStatsResponse1, adStatsResponse1);
        assertNotEquals(null, adStatsResponse1);
        assertNotEquals(1, adStatsResponse1);
        StatsResponse adStatsResponse2 = new StatsResponse();
        assertEquals(adStatsResponse1, adStatsResponse2);
        Map<String, Object> testClusterStats = new HashMap<>();
        testClusterStats.put("test_stat", 1L);
        adStatsResponse1.setClusterStats(testClusterStats);
        assertNotEquals(adStatsResponse1, adStatsResponse2);
    }

    @Test
    public void testHashCode() {
        StatsResponse adStatsResponse1 = new StatsResponse();
        StatsResponse adStatsResponse2 = new StatsResponse();
        assertEquals(adStatsResponse1.hashCode(), adStatsResponse2.hashCode());
        Map<String, Object> testClusterStats = new HashMap<>();
        testClusterStats.put("test_stat", 1L);
        adStatsResponse1.setClusterStats(testClusterStats);
        assertNotEquals(adStatsResponse1.hashCode(), adStatsResponse2.hashCode());
    }

    @Test
    public void testToXContent() throws IOException {
        StatsResponse adStatsResponse = new StatsResponse();
        Map<String, Object> testClusterStats = new HashMap<>();
        testClusterStats.put("test_stat", 1);
        adStatsResponse.setClusterStats(testClusterStats);
        List<StatsNodeResponse> responses = Collections.emptyList();
        List<FailedNodeException> failures = Collections.emptyList();
        StatsNodesResponse adStatsNodesResponse = new StatsNodesResponse(ClusterName.DEFAULT, responses, failures);
        adStatsResponse.setStatsNodesResponse(adStatsNodesResponse);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        adStatsResponse.toXContent(builder);
        XContentParser parser = createParser(builder);
        assertEquals(1, parser.map().get("test_stat"));
    }
}
