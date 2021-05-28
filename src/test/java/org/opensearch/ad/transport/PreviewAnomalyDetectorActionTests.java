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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.transport;

import java.time.Instant;

import org.junit.Assert;
import org.junit.Test;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.xcontent.ToXContent;
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
