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

import java.io.IOException;
import java.time.Instant;

import org.junit.Test;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import com.google.common.collect.ImmutableMap;

public class ValidateAnomalyDetectorRequestTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    @Test
    public void testValidateAnomalyDetectorRequestSerialization() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        TimeValue requestTimeout = new TimeValue(1000L);
        String typeStr = "type";

        ValidateAnomalyDetectorRequest request1 = new ValidateAnomalyDetectorRequest(detector, typeStr, 1, 1, 1, requestTimeout);

        // Test serialization
        BytesStreamOutput output = new BytesStreamOutput();
        request1.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ValidateAnomalyDetectorRequest request2 = new ValidateAnomalyDetectorRequest(input);
        assertEquals("serialization has the wrong detector", request2.getDetector(), detector);
        assertEquals("serialization has the wrong typeStr", request2.getTypeStr(), typeStr);
        assertEquals("serialization has the wrong requestTimeout", request2.getRequestTimeout(), requestTimeout);
    }
}
