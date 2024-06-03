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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.util.RestHandlerUtils;

import com.google.common.collect.ImmutableMap;

public class IndexAnomalyDetectorActionTests extends OpenSearchSingleNodeTestCase {
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    @Test
    public void testIndexRequest() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        IndexAnomalyDetectorRequest request = new IndexAnomalyDetectorRequest(
            "1234",
            4321,
            5678,
            WriteRequest.RefreshPolicy.NONE,
            detector,
            RestRequest.Method.PUT,
            TimeValue.timeValueSeconds(60),
            1000,
            10,
            5,
            10
        );
        request.writeTo(out);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        IndexAnomalyDetectorRequest newRequest = new IndexAnomalyDetectorRequest(input);
        Assert.assertEquals(request.getDetectorID(), newRequest.getDetectorID());
        Assert.assertNull(newRequest.validate());
    }

    @Test
    public void testIndexResponse() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        IndexAnomalyDetectorResponse response = new IndexAnomalyDetectorResponse("1234", 56, 78, 90, detector, RestStatus.OK);
        response.writeTo(out);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        IndexAnomalyDetectorResponse newResponse = new IndexAnomalyDetectorResponse(input);
        Assert.assertEquals(response.getId(), newResponse.getId());
        XContentBuilder builder = TestHelpers.builder();
        Assert.assertNotNull(newResponse.toXContent(builder, ToXContent.EMPTY_PARAMS));

        Map<String, Object> map = TestHelpers.XContentBuilderToMap(builder);
        Assert.assertEquals(map.get(RestHandlerUtils._ID), "1234");
    }
}
