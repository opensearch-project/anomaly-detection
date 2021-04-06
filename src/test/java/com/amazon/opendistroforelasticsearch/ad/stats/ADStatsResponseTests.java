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

package com.amazon.opendistroforelasticsearch.ad.stats;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsNodeResponse;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsNodesResponse;

public class ADStatsResponseTests extends OpenSearchTestCase {
    @Test
    public void testGetAndSetClusterStats() {
        ADStatsResponse adStatsResponse = new ADStatsResponse();
        Map<String, Object> testClusterStats = new HashMap<>();
        testClusterStats.put("test_stat", 1L);
        adStatsResponse.setClusterStats(testClusterStats);
        assertEquals(testClusterStats, adStatsResponse.getClusterStats());
    }

    @Test
    public void testGetAndSetADStatsNodesResponse() {
        ADStatsResponse adStatsResponse = new ADStatsResponse();
        List<ADStatsNodeResponse> responses = Collections.emptyList();
        List<FailedNodeException> failures = Collections.emptyList();
        ADStatsNodesResponse adStatsNodesResponse = new ADStatsNodesResponse(ClusterName.DEFAULT, responses, failures);
        adStatsResponse.setADStatsNodesResponse(adStatsNodesResponse);
        assertEquals(adStatsNodesResponse, adStatsResponse.getADStatsNodesResponse());
    }

    @Test
    public void testMerge() {
        ADStatsResponse adStatsResponse1 = new ADStatsResponse();
        Map<String, Object> testClusterStats = new HashMap<>();
        testClusterStats.put("test_stat", 1L);
        adStatsResponse1.setClusterStats(testClusterStats);

        ADStatsResponse adStatsResponse2 = new ADStatsResponse();
        List<ADStatsNodeResponse> responses = Collections.emptyList();
        List<FailedNodeException> failures = Collections.emptyList();
        ADStatsNodesResponse adStatsNodesResponse = new ADStatsNodesResponse(ClusterName.DEFAULT, responses, failures);
        adStatsResponse2.setADStatsNodesResponse(adStatsNodesResponse);

        adStatsResponse1.merge(adStatsResponse2);
        assertEquals(testClusterStats, adStatsResponse1.getClusterStats());
        assertEquals(adStatsNodesResponse, adStatsResponse1.getADStatsNodesResponse());

        adStatsResponse2.merge(adStatsResponse1);
        assertEquals(testClusterStats, adStatsResponse2.getClusterStats());
        assertEquals(adStatsNodesResponse, adStatsResponse2.getADStatsNodesResponse());

        // Confirm merging with null does nothing
        adStatsResponse1.merge(null);
        assertEquals(testClusterStats, adStatsResponse1.getClusterStats());
        assertEquals(adStatsNodesResponse, adStatsResponse1.getADStatsNodesResponse());

        // Confirm merging with self does nothing
        adStatsResponse1.merge(adStatsResponse1);
        assertEquals(testClusterStats, adStatsResponse1.getClusterStats());
        assertEquals(adStatsNodesResponse, adStatsResponse1.getADStatsNodesResponse());
    }

    @Test
    public void testEquals() {
        ADStatsResponse adStatsResponse1 = new ADStatsResponse();
        assertEquals(adStatsResponse1, adStatsResponse1);
        assertNotEquals(null, adStatsResponse1);
        assertNotEquals(1, adStatsResponse1);
        ADStatsResponse adStatsResponse2 = new ADStatsResponse();
        assertEquals(adStatsResponse1, adStatsResponse2);
        Map<String, Object> testClusterStats = new HashMap<>();
        testClusterStats.put("test_stat", 1L);
        adStatsResponse1.setClusterStats(testClusterStats);
        assertNotEquals(adStatsResponse1, adStatsResponse2);
    }

    @Test
    public void testHashCode() {
        ADStatsResponse adStatsResponse1 = new ADStatsResponse();
        ADStatsResponse adStatsResponse2 = new ADStatsResponse();
        assertEquals(adStatsResponse1.hashCode(), adStatsResponse2.hashCode());
        Map<String, Object> testClusterStats = new HashMap<>();
        testClusterStats.put("test_stat", 1L);
        adStatsResponse1.setClusterStats(testClusterStats);
        assertNotEquals(adStatsResponse1.hashCode(), adStatsResponse2.hashCode());
    }

    @Test
    public void testToXContent() throws IOException {
        ADStatsResponse adStatsResponse = new ADStatsResponse();
        Map<String, Object> testClusterStats = new HashMap<>();
        testClusterStats.put("test_stat", 1);
        adStatsResponse.setClusterStats(testClusterStats);
        List<ADStatsNodeResponse> responses = Collections.emptyList();
        List<FailedNodeException> failures = Collections.emptyList();
        ADStatsNodesResponse adStatsNodesResponse = new ADStatsNodesResponse(ClusterName.DEFAULT, responses, failures);
        adStatsResponse.setADStatsNodesResponse(adStatsNodesResponse);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        adStatsResponse.toXContent(builder);
        XContentParser parser = createParser(builder);
        assertEquals(1, parser.map().get("test_stat"));
    }
}
