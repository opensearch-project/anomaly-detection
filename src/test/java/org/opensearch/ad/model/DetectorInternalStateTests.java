/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

import java.io.IOException;
import java.time.Instant;

import org.opensearch.ad.TestHelpers;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

public class DetectorInternalStateTests extends OpenSearchSingleNodeTestCase {

    public void testToXContentDetectorInternalState() throws IOException {
        DetectorInternalState internalState = new DetectorInternalState.Builder()
            .lastUpdateTime(Instant.ofEpochMilli(100L))
            .error("error-test")
            .build();
        String internalStateString = TestHelpers
            .xContentBuilderToString(internalState.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        DetectorInternalState parsedInternalState = DetectorInternalState.parse(TestHelpers.parser(internalStateString));
        assertEquals(internalState, parsedInternalState);
    }
}
