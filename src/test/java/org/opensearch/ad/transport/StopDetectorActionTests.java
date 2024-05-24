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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.timeseries.transport.StopConfigRequest;
import org.opensearch.timeseries.transport.StopConfigResponse;

public class StopDetectorActionTests extends OpenSearchIntegTestCase {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testStopDetectorAction() {
        Assert.assertNotNull(StopDetectorAction.INSTANCE.name());
        Assert.assertEquals(StopDetectorAction.INSTANCE.name(), StopDetectorAction.NAME);
    }

    @Test
    public void fromActionRequest_Success() {
        StopConfigRequest stopDetectorRequest = new StopConfigRequest("adID");
        ActionRequest actionRequest = new ActionRequest() {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                stopDetectorRequest.writeTo(out);
            }
        };
        StopConfigRequest result = StopConfigRequest.fromActionRequest(actionRequest);
        assertNotSame(result, stopDetectorRequest);
        assertEquals(result.getConfigID(), stopDetectorRequest.getConfigID());
    }

    @Test
    public void writeTo_Success() throws IOException {
        BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
        StopConfigResponse response = new StopConfigResponse(true);
        response.writeTo(bytesStreamOutput);
        StopConfigResponse parsedResponse = new StopConfigResponse(bytesStreamOutput.bytes().streamInput());
        assertNotEquals(response, parsedResponse);
        assertEquals(response.success(), parsedResponse.success());
    }

    @Test
    public void fromActionResponse_Success() throws IOException {
        StopConfigResponse stopDetectorResponse = new StopConfigResponse(true);
        ActionResponse actionResponse = new ActionResponse() {
            @Override
            public void writeTo(StreamOutput streamOutput) throws IOException {
                stopDetectorResponse.writeTo(streamOutput);
            }
        };
        StopConfigResponse result = stopDetectorResponse.fromActionResponse(actionResponse);
        assertNotSame(result, stopDetectorResponse);
        assertEquals(result.success(), stopDetectorResponse.success());

        StopConfigResponse parsedStopDetectorResponse = stopDetectorResponse.fromActionResponse(stopDetectorResponse);
        assertEquals(parsedStopDetectorResponse, stopDetectorResponse);
    }

    @Test
    public void toXContentTest() throws IOException {
        StopConfigResponse stopDetectorResponse = new StopConfigResponse(true);
        XContentBuilder builder = MediaTypeRegistry.contentBuilder(XContentType.JSON);
        stopDetectorResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertNotNull(builder);
        String jsonStr = builder.toString();
        assertEquals("{\"success\":true}", jsonStr);
    }
}
