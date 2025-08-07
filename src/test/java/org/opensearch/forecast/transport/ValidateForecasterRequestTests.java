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

import org.junit.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.transport.ValidateConfigRequest;

/**
 * Unit tests that cover the Forecast branch and the
 * UnsupportedOperationException branch in
 * {@link ValidateConfigRequest#ValidateConfigRequest(StreamInput)}.
 */
public class ValidateForecasterRequestTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    /**
     * Verifies that a {@link ValidateConfigRequest} built with
     * {@link AnalysisType#FORECAST} serialises and deserialises correctly
     * (i.e. the “else if (context.isForecast()) { … }” branch is executed).
     */
    @Test
    public void testValidateForecasterRequestSerialization() throws IOException {
        Forecaster forecaster = TestHelpers.randomForecaster();
        TimeValue requestTimeout = TimeValue.timeValueSeconds(2);
        String validationType = "type";

        ValidateConfigRequest original = new ValidateConfigRequest(
            AnalysisType.FORECAST,
            forecaster,
            validationType,
            1,   // max single‑stream configs
            1,   // max HC configs
            1,   // max features
            requestTimeout,
            10   // max categorical fields
        );

        /* --------  round‑trip serialise / deserialise  -------- */
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        ValidateConfigRequest parsed = new ValidateConfigRequest(in);

        /* -------------------  assertions  -------------------- */
        assertEquals("Forecaster lost during (de)serialisation", forecaster, parsed.getConfig());
        assertEquals("Validation type corrupted", validationType, parsed.getValidationType());
        assertEquals("Timeout corrupted", requestTimeout, parsed.getRequestTimeout());
    }

    /**
     * Verifies that an unsupported analysis type triggers the
     * UnsupportedOperationException branch.
     *
     * NOTE:  In the current code‑base the enum contains AD and FORECAST.
     *        If/when another value is added (e.g. TREND or UNKNOWN) that
     *        does **not** identify as AD or FORECAST, replace the constant
     *        below with that value.
     */
    @Test
    public void testUnsupportedAnalysisTypeThrows() throws IOException {
        Forecaster forecaster = TestHelpers.randomForecaster();
        TimeValue requestTimeout = TimeValue.timeValueSeconds(2);
        String validationType = "type";

        ValidateConfigRequest original = new ValidateConfigRequest(
            AnalysisType.UNKNOWN,
            forecaster,
            validationType,
            1,   // max single‑stream configs
            1,   // max HC configs
            1,   // max features
            requestTimeout,
            10   // max categorical fields
        );

        /* --------  round‑trip serialise / deserialise  -------- */
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        assertThrows(UnsupportedOperationException.class, () -> new ValidateConfigRequest(in));
    }
}
