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

/*
package org.opensearch.ad.transport;


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
}
*/
