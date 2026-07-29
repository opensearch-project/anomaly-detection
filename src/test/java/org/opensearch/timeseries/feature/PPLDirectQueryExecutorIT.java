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

package org.opensearch.timeseries.feature;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Constructor;

import org.opensearch.action.ActionRequest;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class PPLDirectQueryExecutorIT extends OpenSearchTestCase {

    public void testPPLTransportRequestRoundTripsThroughStreamConstructor() throws Exception {
        ActionRequest request = newPPLTransportRequest(
            "source = logs | stats count() as count by span(timestamp, 1m) as bucket",
            "jdbc",
            "/_plugins/_ppl"
        );

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (OutputStreamStreamOutput output = new OutputStreamStreamOutput(baos)) {
            request.writeTo(output);
        }
        try (InputStreamStreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
            Object roundTrippedRequest = pplTransportRequestStreamConstructor().newInstance(input);
            assertEquals(readPrivateField(request, "query"), readPrivateField(roundTrippedRequest, "query"));
            assertEquals(readPrivateField(request, "format"), readPrivateField(roundTrippedRequest, "format"));
            assertNull(readPrivateField(roundTrippedRequest, "explainMode"));
            assertNull(readPrivateField(roundTrippedRequest, "jsonContent"));
            assertEquals(readPrivateField(request, "path"), readPrivateField(roundTrippedRequest, "path"));
            assertTrue((Boolean) readPrivateField(roundTrippedRequest, "sanitize"));
            assertFalse((Boolean) readPrivateField(roundTrippedRequest, "profile"));
            assertFalse((Boolean) readPrivateField(roundTrippedRequest, "analyze"));
            assertNull(readPrivateField(roundTrippedRequest, "queryId"));
        }
    }

    private static ActionRequest newPPLTransportRequest(String query, String format, String path) throws Exception {
        return (ActionRequest) pplTransportRequestConstructor().newInstance(query, format, path);
    }

    private static Constructor<?> pplTransportRequestConstructor() throws Exception {
        Constructor<?> constructor = pplTransportRequestClass().getDeclaredConstructor(String.class, String.class, String.class);
        constructor.setAccessible(true);
        return constructor;
    }

    private static Constructor<?> pplTransportRequestStreamConstructor() throws Exception {
        Constructor<?> constructor = pplTransportRequestClass().getDeclaredConstructor(StreamInput.class);
        constructor.setAccessible(true);
        return constructor;
    }

    private static Class<?> pplTransportRequestClass() throws ClassNotFoundException {
        return Class.forName("org.opensearch.timeseries.feature.PPLDirectQueryExecutor$PPLTransportRequest");
    }

    private static Object readPrivateField(Object target, String fieldName) throws Exception {
        java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}
