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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.transport.StatsNodeResponse;
import org.opensearch.timeseries.transport.StatsNodesResponse;
import org.opensearch.timeseries.transport.StatsResponse;
import org.opensearch.timeseries.transport.StatsTimeSeriesResponse;

public class StatsAnomalyDetectorActionTests extends OpenSearchTestCase {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testStatsAction() {
        Assert.assertNotNull(StatsAnomalyDetectorAction.INSTANCE.name());
        Assert.assertEquals(StatsAnomalyDetectorAction.INSTANCE.name(), StatsAnomalyDetectorAction.NAME);
    }

    @Test
    public void testStatsResponse() throws IOException {
        StatsResponse adStatsResponse = new StatsResponse();
        Map<String, Object> testClusterStats = new HashMap<>();
        testClusterStats.put("test_response", 1);
        adStatsResponse.setClusterStats(testClusterStats);
        List<StatsNodeResponse> responses = Collections.emptyList();
        List<FailedNodeException> failures = Collections.emptyList();
        StatsNodesResponse adStatsNodesResponse = new StatsNodesResponse(ClusterName.DEFAULT, responses, failures);
        adStatsResponse.setStatsNodesResponse(adStatsNodesResponse);

        StatsTimeSeriesResponse response = new StatsTimeSeriesResponse(adStatsResponse);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        StatsTimeSeriesResponse newResponse = new StatsTimeSeriesResponse(input);
        assertNotNull(newResponse);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = newResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = createParser(builder);
        assertEquals(1, parser.map().get("test_response"));
    }
}
