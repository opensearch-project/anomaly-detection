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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class ADResultBulkResponseTests extends OpenSearchTestCase {
    public void testSerialization() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        List<IndexRequest> retryRequests = new ArrayList<>();
        retryRequests.add(new IndexRequest("index").id("blah").source(Collections.singletonMap("foo", "bar")));
        ADResultBulkResponse response = new ADResultBulkResponse(retryRequests);
        response.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        ADResultBulkResponse readResponse = new ADResultBulkResponse(streamInput);
        assertTrue(readResponse.hasFailures());
    }
}
