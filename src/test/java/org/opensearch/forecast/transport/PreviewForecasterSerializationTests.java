/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors.
 */

package org.opensearch.forecast.transport;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.timeseries.TestHelpers;

public class PreviewForecasterSerializationTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testPreviewForecasterRequestRoundTrip() throws IOException {
        Forecaster forecaster = TestHelpers.randomForecaster();
        Instant periodStart = Instant.now().minusSeconds(600);
        Instant periodEnd = Instant.now();

        PreviewForecasterRequest original = new PreviewForecasterRequest(forecaster, forecaster.getId(), periodStart, periodEnd);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        PreviewForecasterRequest parsed = new PreviewForecasterRequest(in);

        assertEquals(forecaster, parsed.getForecaster());
        assertEquals(forecaster.getId(), parsed.getForecasterId());
        assertEquals(periodStart, parsed.getPeriodStart());
        assertEquals(periodEnd, parsed.getPeriodEnd());
    }

    public void testPreviewForecasterResponseRoundTrip() throws IOException {
        Forecaster forecaster = TestHelpers.randomForecaster();
        PreviewForecasterResponse original = new PreviewForecasterResponse(Collections.emptyList(), forecaster);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        PreviewForecasterResponse parsed = new PreviewForecasterResponse(in);

        assertTrue(parsed.getForecastResult().isEmpty());
        assertEquals(forecaster, parsed.getForecaster());
    }
}
