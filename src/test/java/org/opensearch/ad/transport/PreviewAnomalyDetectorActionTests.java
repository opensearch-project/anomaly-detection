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

import java.time.Instant;

import org.junit.Assert;
import org.junit.Test;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class PreviewAnomalyDetectorActionTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    @Test
    public void testPreviewRequest() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        PreviewAnomalyDetectorRequest request = new PreviewAnomalyDetectorRequest(
            detector,
            "1234",
            Instant.now().minusSeconds(60),
            Instant.now()
        );
        request.writeTo(out);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        PreviewAnomalyDetectorRequest newRequest = new PreviewAnomalyDetectorRequest(input);
        Assert.assertEquals(request.getDetectorId(), newRequest.getDetectorId());
        Assert.assertEquals(request.getStartTime(), newRequest.getStartTime());
        Assert.assertEquals(request.getEndTime(), newRequest.getEndTime());
        Assert.assertNotNull(newRequest.getDetector());
        Assert.assertNull(newRequest.validate());
    }

    @Test
    public void testPreviewResponse() throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        AnomalyResult result = TestHelpers.randomHCADAnomalyDetectResult(0.8d, 0d);
        PreviewAnomalyDetectorResponse response = new PreviewAnomalyDetectorResponse(ImmutableList.of(result), detector);
        response.writeTo(out);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        PreviewAnomalyDetectorResponse newResponse = new PreviewAnomalyDetectorResponse(input);
        Assert.assertNotNull(newResponse.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
    }

    @Test
    public void testPreviewAction() throws Exception {
        Assert.assertNotNull(PreviewAnomalyDetectorAction.INSTANCE.name());
        Assert.assertEquals(PreviewAnomalyDetectorAction.INSTANCE.name(), PreviewAnomalyDetectorAction.NAME);
    }
}
