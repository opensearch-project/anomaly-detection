/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

import static org.opensearch.ad.model.AnomalyResultBucket.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.TestHelpers;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

public class AnomalyResultBucketTests extends AbstractADTest {

    public void testSerializeAnomalyResultBucket() throws IOException {
        AnomalyResultBucket anomalyResultBucket = TestHelpers.randomAnomalyResultBucket();
        BytesStreamOutput output = new BytesStreamOutput();
        anomalyResultBucket.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        AnomalyResultBucket parsedAnomalyResultBucket = new AnomalyResultBucket(input);
        assertTrue(parsedAnomalyResultBucket.equals(anomalyResultBucket));
    }

    public void testAnomalyResultBucketEquals() {
        Map<String, Object> keyOne = new HashMap<>();
        keyOne.put("test-field-1", "test-value-1");
        Map<String, Object> keyTwo = new HashMap<>();
        keyTwo.put("test-field-2", "test-value-2");
        AnomalyResultBucket testBucketOne = new AnomalyResultBucket(keyOne, 3, 0.5);
        AnomalyResultBucket testBucketTwo = new AnomalyResultBucket(keyOne, 5, 0.75);
        AnomalyResultBucket testBucketThree = new AnomalyResultBucket(keyTwo, 7, 0.2);
        assertFalse(testBucketOne.equals(testBucketTwo));
        assertFalse(testBucketTwo.equals(testBucketThree));
    }

    @SuppressWarnings("unchecked")
    public void testToXContent() throws IOException {
        Map<String, Object> key = new HashMap<>() {
            {
                put("test-field-1", "test-value-1");
            }
        };
        int docCount = 5;
        double maxAnomalyGrade = 0.5;
        AnomalyResultBucket testBucket = new AnomalyResultBucket(key, docCount, maxAnomalyGrade);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        testBucket.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = createParser(builder);
        Map<String, Object> parsedMap = parser.map();

        assertEquals(testBucket.getKey().get("test-field-1"), ((Map<String, Object>) parsedMap.get(KEY_FIELD)).get("test-field-1"));
        assertEquals(testBucket.getDocCount(), parsedMap.get(DOC_COUNT_FIELD));
        assertEquals(maxAnomalyGrade, (Double) parsedMap.get(MAX_ANOMALY_GRADE_FIELD), 0.000001d);
    }
}
