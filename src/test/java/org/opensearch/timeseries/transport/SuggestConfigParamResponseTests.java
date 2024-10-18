/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.timeseries.transport;

import java.io.IOException;
import java.time.temporal.ChronoUnit;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Mergeable;

public class SuggestConfigParamResponseTests extends OpenSearchTestCase {

    /**
     * Test the serialization and deserialization of SuggestConfigParamResponse.
     * This covers both the writeTo(StreamOutput out) method and the
     * SuggestConfigParamResponse(StreamInput in) constructor.
     */
    public void testSerializationDeserialization() throws IOException {
        // Create an instance of SuggestConfigParamResponse
        IntervalTimeConfiguration interval = new IntervalTimeConfiguration(10, ChronoUnit.MINUTES);
        Integer horizon = 12;
        Integer history = 24;

        SuggestConfigParamResponse originalResponse = new SuggestConfigParamResponse(interval, horizon, history);

        // Serialize it to a BytesStreamOutput
        BytesStreamOutput out = new BytesStreamOutput();
        originalResponse.writeTo(out);

        // Deserialize it from the StreamInput
        StreamInput in = out.bytes().streamInput();
        SuggestConfigParamResponse deserializedResponse = new SuggestConfigParamResponse(in);

        // Assert that the deserialized object matches the original
        assertEquals(originalResponse.getInterval(), deserializedResponse.getInterval());
        assertEquals(originalResponse.getHorizon(), deserializedResponse.getHorizon());
        assertEquals(originalResponse.getHistory(), deserializedResponse.getHistory());
    }

    /**
     * Test the toXContent(XContentBuilder builder) method.
     * This ensures that the response is correctly converted to XContent.
     */
    public void testToXContent() throws IOException {
        IntervalTimeConfiguration interval = new IntervalTimeConfiguration(10, ChronoUnit.MINUTES);
        Integer horizon = 12;
        Integer history = 24;

        SuggestConfigParamResponse response = new SuggestConfigParamResponse(interval, horizon, history);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder);
        String jsonString = builder.toString();

        // Expected JSON string contains interval, horizon, history
        assertTrue("actual json: " + jsonString, jsonString.contains("\"interval\""));
        assertTrue("actual json: " + jsonString, jsonString.contains("\"interval\":10"));
        assertTrue("actual json: " + jsonString, jsonString.contains("\"unit\":\"Minutes\""));
        assertTrue("actual json: " + jsonString, jsonString.contains("\"horizon\":12"));
        assertTrue("actual json: " + jsonString, jsonString.contains("\"history\":24"));
    }

    /**
     * Test the merge(Mergeable other) method when it returns early due to:
     * - other being null
     * - this being equal to other
     * - getClass() != other.getClass()
     */
    public void testMerge_ReturnEarly() {
        IntervalTimeConfiguration interval = new IntervalTimeConfiguration(10, ChronoUnit.MINUTES);
        Integer horizon = 12;
        Integer history = 24;

        SuggestConfigParamResponse response = new SuggestConfigParamResponse(interval, horizon, history);

        // Case when other == null
        response.merge(null);

        // Response should remain unchanged
        assertEquals(interval, response.getInterval());
        assertEquals(horizon, response.getHorizon());
        assertEquals(history, response.getHistory());

        // Case when this == other
        response.merge(response);

        // Response should remain unchanged
        assertEquals(interval, response.getInterval());
        assertEquals(horizon, response.getHorizon());
        assertEquals(history, response.getHistory());

        // Case when getClass() != other.getClass()
        Mergeable other = new Mergeable() {
            @Override
            public void merge(Mergeable other) {
                // No operation
            }
        };

        response.merge(other);

        // Response should remain unchanged
        assertEquals(interval, response.getInterval());
        assertEquals(horizon, response.getHorizon());
        assertEquals(history, response.getHistory());
    }

    /**
     * Test the merge(Mergeable other) method when otherProfile.getHistory() != null.
     * This ensures that the history field is correctly updated from the other object.
     */
    public void testMerge_OtherHasHistory() {
        IntervalTimeConfiguration interval = new IntervalTimeConfiguration(10, ChronoUnit.MINUTES);
        Integer horizon = 12;
        Integer history = null; // Initial history is null

        SuggestConfigParamResponse response = new SuggestConfigParamResponse(interval, horizon, history);

        Integer otherHistory = 30;

        SuggestConfigParamResponse otherResponse = new SuggestConfigParamResponse(null, null, otherHistory);

        // Before merge, response.history is null
        assertNull(response.getHistory());

        // Merge
        response.merge(otherResponse);

        // After merge, response.history should be updated
        assertEquals(otherHistory, response.getHistory());

        // Interval and horizon should remain unchanged
        assertEquals(interval, response.getInterval());
        assertEquals(horizon, response.getHorizon());
    }
}
