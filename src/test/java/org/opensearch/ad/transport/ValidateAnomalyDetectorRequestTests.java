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

import org.junit.Test;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.transport.ValidateConfigRequest;

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

        ValidateConfigRequest request1 = new ValidateConfigRequest(AnalysisType.AD, detector, typeStr, 1, 1, 1, requestTimeout, 10);

        // Test serialization
        BytesStreamOutput output = new BytesStreamOutput();
        request1.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ValidateConfigRequest request2 = new ValidateConfigRequest(input);
    }

    @Test
    public void testValidateAnomalyDetectorRequestWithNullValues() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        String typeStr = "model";

        // Test convenience constructor with null cluster settings
        ValidateConfigRequest request1 = new ValidateConfigRequest(AnalysisType.AD, detector, typeStr);

        // Test serialization with null values
        BytesStreamOutput output = new BytesStreamOutput();
        request1.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ValidateConfigRequest request2 = new ValidateConfigRequest(input);

        assertEquals("serialization has the wrong detector", request2.getConfig(), detector);
        assertEquals("serialization has the wrong validation type", request2.getValidationType(), typeStr);
        assertNull("maxSingleStreamConfigs should be null", request2.getMaxSingleEntityAnomalyDetectors());
        assertNull("maxHCConfigs should be null", request2.getMaxMultiEntityAnomalyDetectors());
        assertNull("maxFeatures should be null", request2.getMaxAnomalyFeatures());

        assertEquals("serialization has the wrong typeStr", request2.getValidationType(), typeStr);
        assertEquals("serialization has the wrong requestTimeout", request2.getRequestTimeout(), TimeValue.timeValueSeconds(60));
    }
}
