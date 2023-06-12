/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import java.io.IOException;

import org.junit.Before;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.model.DataByFeatureId;

public class DataByFeatureIdTests extends OpenSearchTestCase {
    String expectedFeatureId = "testFeature";
    Double expectedData = 123.45;
    DataByFeatureId dataByFeatureId;

    @Before
    public void setup() {
        dataByFeatureId = new DataByFeatureId(expectedFeatureId, expectedData);
    }

    public void testInputOutputStream() throws IOException {

        BytesStreamOutput output = new BytesStreamOutput();
        dataByFeatureId.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        DataByFeatureId restoredDataByFeatureId = new DataByFeatureId(streamInput);
        assertEquals(expectedFeatureId, restoredDataByFeatureId.getFeatureId());
        assertEquals(expectedData, restoredDataByFeatureId.getData());
    }

    public void testToXContent() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        dataByFeatureId.toXContent(builder, ToXContent.EMPTY_PARAMS);

        XContentParser parser = createParser(builder);
        // advance to first token
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IOException("Expected data to start with an Object");
        }

        DataByFeatureId parsedDataByFeatureId = DataByFeatureId.parse(parser);

        assertEquals(expectedFeatureId, parsedDataByFeatureId.getFeatureId());
        assertEquals(expectedData, parsedDataByFeatureId.getData());
    }

    public void testEqualsAndHashCode() {
        DataByFeatureId dataByFeatureId1 = new DataByFeatureId("feature1", 1.0);
        DataByFeatureId dataByFeatureId2 = new DataByFeatureId("feature1", 1.0);
        DataByFeatureId dataByFeatureId3 = new DataByFeatureId("feature2", 2.0);

        // Test equal objects are equal
        assertEquals(dataByFeatureId1, dataByFeatureId2);
        assertEquals(dataByFeatureId1.hashCode(), dataByFeatureId2.hashCode());

        // Test unequal objects are not equal
        assertNotEquals(dataByFeatureId1, dataByFeatureId3);
        assertNotEquals(dataByFeatureId1.hashCode(), dataByFeatureId3.hashCode());

        // Test object is not equal to null
        assertNotEquals(dataByFeatureId1, null);

        // Test object is not equal to object of different type
        assertNotEquals(dataByFeatureId1, "string");

        // Test object is equal to itself
        assertEquals(dataByFeatureId1, dataByFeatureId1);
        assertEquals(dataByFeatureId1.hashCode(), dataByFeatureId1.hashCode());
    }
}
