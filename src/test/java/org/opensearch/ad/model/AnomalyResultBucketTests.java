/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

import java.io.IOException;

import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.TestHelpers;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;

public class AnomalyResultBucketTests extends AbstractADTest {

    public void testSerializeAnomalyResultBucket() throws IOException {
        AnomalyResultBucket anomalyResultBucket = TestHelpers.randomAnomalyResultBucket();
        BytesStreamOutput output = new BytesStreamOutput();
        anomalyResultBucket.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        AnomalyResultBucket parsedAnomalyResultBucket = new AnomalyResultBucket(input);
        assertTrue(parsedAnomalyResultBucket.equals(anomalyResultBucket));
    }
}
