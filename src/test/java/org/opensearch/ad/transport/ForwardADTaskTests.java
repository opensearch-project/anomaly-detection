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

/*


public class ForwardADTaskTests extends OpenSearchSingleNodeTestCase {
    private Version testVersion;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        testVersion = Version.fromString("1.1.0");
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, AnomalyDetectorPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testForwardADTaskRequest() throws IOException {
        ForwardADTaskRequest request = new ForwardADTaskRequest(
            TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now()),
            TestHelpers.randomDetectionDateRange(),
            TestHelpers.randomUser(),
            ADTaskAction.START,
            randomInt(),
            testVersion
        );
        testForwardADTaskRequest(request);
    }

    */
/*public void testForwardADTaskRequestWithoutUser() throws IOException {
        ForwardADTaskRequest request = new ForwardADTaskRequest(
            TestHelpers.randomAnomalyDetector(ImmutableMap.of(), Instant.now()),
            TestHelpers.randomDetectionDateRange(),
            null,
            ADTaskAction.START,
            randomInt(),
            testVersion
        );
        testForwardADTaskRequest(request);
    }*//*
        
        
        */
/*public void testInvalidForwardADTaskRequest() {
        ForwardADTaskRequest request = new ForwardADTaskRequest(
            null,
            TestHelpers.randomDetectionDateRange(),
            TestHelpers.randomUser(),
            ADTaskAction.START,
            randomInt(),
            testVersion
        );

        ActionRequestValidationException exception = request.validate();
        assertTrue(exception.getMessage().contains(CommonErrorMessages.DETECTOR_MISSING));
    }*//*
        
        
        */
/*private void testForwardADTaskRequest(ForwardADTaskRequest request) throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ForwardADTaskRequest parsedRequest = new ForwardADTaskRequest(input);
        if (request.getUser() != null) {
            assertTrue(request.getUser().equals(parsedRequest.getUser()));
        } else {
            assertNull(parsedRequest.getUser());
        }
    }*//*
        
        }
        */
