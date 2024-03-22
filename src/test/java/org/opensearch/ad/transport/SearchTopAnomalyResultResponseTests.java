/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.TestHelpers;

public class SearchTopAnomalyResultResponseTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        SearchTopAnomalyResultResponse originalResponse = new SearchTopAnomalyResultResponse(
            Arrays.asList(TestHelpers.randomAnomalyResultBucket())
        );

        BytesStreamOutput output = new BytesStreamOutput();
        originalResponse.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        SearchTopAnomalyResultResponse parsedResponse = new SearchTopAnomalyResultResponse(input);
        assertEquals(originalResponse.getAnomalyResultBuckets(), parsedResponse.getAnomalyResultBuckets());
    }

    public void testEmptyResults() {
        SearchTopAnomalyResultResponse response = new SearchTopAnomalyResultResponse(new ArrayList<>());
    }

    public void testPopulatedResults() {
        SearchTopAnomalyResultResponse response = new SearchTopAnomalyResultResponse(
            Arrays.asList(TestHelpers.randomAnomalyResultBucket())
        );
    }
}
