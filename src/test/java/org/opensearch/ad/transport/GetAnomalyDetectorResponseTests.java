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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;

import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.TestHelpers;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class GetAnomalyDetectorResponseTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, AnomalyDetectorPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testConstructor() throws IOException {
        GetAnomalyDetectorResponse response = createGetAnomalyDetectorResponse(false, false);
        assertNull(response.getAdJob());
        assertNull(response.getRealtimeAdTask());
        assertNull(response.getHistoricalAdTask());
        response = createGetAnomalyDetectorResponse(true, true);
        assertNotNull(response.getAdJob());
        assertNotNull(response.getRealtimeAdTask());
        assertNotNull(response.getHistoricalAdTask());
    }

    public void testSerializationWithoutJobAndTask() throws IOException {
        GetAnomalyDetectorResponse response = createGetAnomalyDetectorResponse(false, false);
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        GetAnomalyDetectorResponse parsedResponse = new GetAnomalyDetectorResponse(input);
        assertNull(parsedResponse.getAdJob());
        assertNull(parsedResponse.getRealtimeAdTask());
        assertNull(parsedResponse.getHistoricalAdTask());
        assertEquals(response.getDetector(), parsedResponse.getDetector());
    }

    public void testSerializationWithJobAndTask() throws IOException {
        GetAnomalyDetectorResponse response = createGetAnomalyDetectorResponse(true, true);
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        GetAnomalyDetectorResponse parsedResponse = new GetAnomalyDetectorResponse(input);
        assertNotNull(parsedResponse.getAdJob());
        assertNotNull(parsedResponse.getRealtimeAdTask());
        assertNotNull(parsedResponse.getHistoricalAdTask());
        assertEquals(response.getDetector(), parsedResponse.getDetector());
    }

    public void testFromActionResponse() throws IOException {
        GetAnomalyDetectorResponse response = createGetAnomalyDetectorResponse(true, true);
        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());

        GetAnomalyDetectorResponse reserializedResponse = GetAnomalyDetectorResponse
            .fromActionResponse((ActionResponse) response, writableRegistry());
        assertEquals(response, reserializedResponse);

        ActionResponse invalidActionResponse = new TestActionResponse(input);
        assertThrows(Exception.class, () -> GetAnomalyDetectorResponse.fromActionResponse(invalidActionResponse, writableRegistry()));

    }

    private GetAnomalyDetectorResponse createGetAnomalyDetectorResponse(boolean returnJob, boolean returnTask) throws IOException {
        GetAnomalyDetectorResponse response = new GetAnomalyDetectorResponse(
            randomLong(),
            randomAlphaOfLength(5),
            randomLong(),
            randomLong(),
            TestHelpers.randomAnomalyDetector(ImmutableList.of(), ImmutableMap.of(), Instant.now().truncatedTo(ChronoUnit.SECONDS)),
            TestHelpers.randomAnomalyDetectorJob(),
            returnJob,
            TestHelpers.randomAdTask(),
            TestHelpers.randomAdTask(),
            returnTask,
            RestStatus.OK,
            null,
            null,
            false
        );
        return response;
    }

    // A test ActionResponse class with an inactive writeTo class. Used to ensure exceptions
    // are thrown when parsing implementations of such class.
    private class TestActionResponse extends ActionResponse {
        public TestActionResponse(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            return;
        }
    }
}
