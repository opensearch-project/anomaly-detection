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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.PPLSource;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.client.Client;

public class PPLDirectQueryExecutorTests extends OpenSearchTestCase {

    private Client client;
    private Config config;
    private TestSecurityClientUtil clientUtil;
    private PPLDirectQueryExecutor executor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        config = mock(Config.class);
        when(config.getId()).thenReturn("detector-id");
        when(config.getPPLSource())
            .thenReturn(
                new PPLSource(
                    "PPL",
                    "source = logs | where status >= 400 | stats count() as error_count, sum(bytes) as byte_sum by span(timestamp, 1m)"
                )
            );
        clientUtil = new TestSecurityClientUtil();
        executor = new PPLDirectQueryExecutor(client, clientUtil);
    }

    public void testExecuteMetricQueryParsesNumericAndTextMetricValues() throws Exception {
        clientUtil.response = new RawPPLActionResponse("{\"datarows\":[[42.5,\"7.25\"]]}");
        CapturingListener<Optional<double[]>> listener = new CapturingListener<>();

        executor.executeMetricQuery(config, 1_000L, 2_000L, AnalysisType.AD, listener);

        assertNull(listener.failure);
        assertTrue(listener.response.isPresent());
        assertArrayEquals(new double[] { 42.5, 7.25 }, listener.response.get(), 0.001);
        assertEquals(2, clientUtil.configCallCount);
        assertEquals("detector-id", clientUtil.capturedConfigId);
        assertEquals(0, clientUtil.userCallCount);
        assertSourceAccessRequestTargetsLogs();
        assertSerializedRequestContains(clientUtil.capturedRequest, "timestamp >= \"1970-01-01 00:00:01.000\"");
    }

    public void testExecuteMetricQueryReturnsEmptyForMissingRows() {
        clientUtil.response = new RawPPLActionResponse("{\"datarows\":[]}");
        CapturingListener<Optional<double[]>> listener = new CapturingListener<>();

        executor.executeMetricQuery(config, 1_000L, 2_000L, AnalysisType.AD, listener);

        assertNull(listener.failure);
        assertFalse(listener.response.isPresent());
    }

    public void testExecuteMetricQueryReturnsNaNForNullMetricValue() {
        clientUtil.response = new RawPPLActionResponse("{\"datarows\":[[null,1]]}");
        CapturingListener<Optional<double[]>> listener = new CapturingListener<>();

        executor.executeMetricQuery(config, 1_000L, 2_000L, AnalysisType.AD, listener);

        assertNull(listener.failure);
        assertTrue(listener.response.isPresent());
        assertTrue(Double.isNaN(listener.response.get()[0]));
        assertEquals(1.0, listener.response.get()[1], 0.001);
    }

    public void testExecuteMetricQueryBySpanParsesBucketedMetricValues() throws Exception {
        clientUtil.response = new RawPPLActionResponse(
            "{\"datarows\":[[42.5,\"7.25\",\"1970-01-01 00:01:00\"],[null,1,\"1970-01-01 00:02:00\"],[3,4,null],[5,6]]}"
        );
        CapturingListener<Map<Long, Optional<double[]>>> listener = new CapturingListener<>();

        executor.executeMetricQueryBySpan(config, 60_000L, 180_000L, AnalysisType.AD, listener);

        assertNull(listener.failure);
        assertEquals(2, listener.response.size());
        assertTrue(listener.response.get(60_000L).isPresent());
        assertArrayEquals(new double[] { 42.5, 7.25 }, listener.response.get(60_000L).get(), 0.001);
        assertTrue(listener.response.get(120_000L).isPresent());
        assertTrue(Double.isNaN(listener.response.get(120_000L).get()[0]));
        assertEquals(1.0, listener.response.get(120_000L).get()[1], 0.001);
        assertSerializedRequestContains(clientUtil.capturedRequest, "by span(timestamp, 1m) as bucket");
        assertSerializedRequestContains(clientUtil.capturedRequest, "| sort bucket asc");
    }

    public void testExecuteMetricQueryUsesUserSecurityWhenConfigHasUser() throws Exception {
        User user = new User("alice", Collections.emptyList(), Collections.singletonList("role"), Collections.emptyList());
        when(config.getUser()).thenReturn(user);
        clientUtil.response = new RawPPLActionResponse("{\"datarows\":[[42.5,\"7.25\"]]}");
        CapturingListener<Optional<double[]>> listener = new CapturingListener<>();

        executor.executeMetricQuery(config, 1_000L, 2_000L, AnalysisType.AD, listener);

        assertNull(listener.failure);
        assertTrue(listener.response.isPresent());
        assertEquals(2, clientUtil.userCallCount);
        assertEquals(user, clientUtil.capturedUser);
        assertEquals(0, clientUtil.configCallCount);
        assertSourceAccessRequestTargetsLogs();
    }

    public void testExecuteMetricQueryFailsOnInvalidMetricValue() {
        clientUtil.response = new RawPPLActionResponse("{\"datarows\":[[\"not-a-number\",1]]}");
        CapturingListener<Optional<double[]>> listener = new CapturingListener<>();

        executor.executeMetricQuery(config, 1_000L, 2_000L, AnalysisType.AD, listener);

        assertNotNull(listener.failure);
        assertTrue(listener.failure instanceof NumberFormatException);
    }

    public void testExecuteMetricQueryFailsWhenQueryCannotCompile() {
        when(config.getPPLSource()).thenReturn(new PPLSource("PPL", ""));
        CapturingListener<Optional<double[]>> listener = new CapturingListener<>();

        executor.executeMetricQuery(config, 1_000L, 2_000L, AnalysisType.AD, listener);

        assertNotNull(listener.failure);
        assertTrue(listener.failure instanceof IllegalArgumentException);
        assertEquals(0, clientUtil.configCallCount);
        assertEquals(0, clientUtil.userCallCount);
    }

    public void testExecuteLatestDataTimeQueryUsesUserSecurityAndParsesTimestampText() throws Exception {
        clientUtil.response = new RawPPLActionResponse("{\"datarows\":[[\"1970-01-01 00:00:01.123\"]]}");
        CapturingListener<Optional<Long>> listener = new CapturingListener<>();
        User user = new User("alice", Collections.emptyList(), Collections.singletonList("role"), Collections.emptyList());

        executor.executeLatestDataTimeQuery(user, config, AnalysisType.AD, listener);

        assertNull(listener.failure);
        assertTrue(listener.response.isPresent());
        assertEquals(1_123L, listener.response.get().longValue());
        assertEquals(2, clientUtil.userCallCount);
        assertEquals(user, clientUtil.capturedUser);
        assertEquals(0, clientUtil.configCallCount);
        assertSourceAccessRequestTargetsLogs();
        assertSerializedRequestContains(clientUtil.capturedRequest, "max(timestamp) as latest_time");
    }

    public void testExecuteMinDataTimeQueryParsesNumericTimestamp() {
        clientUtil.response = new RawPPLActionResponse("{\"datarows\":[[12345]]}");
        CapturingListener<Optional<Long>> listener = new CapturingListener<>();

        executor.executeMinDataTimeQuery(config, AnalysisType.AD, listener);

        assertNull(listener.failure);
        assertTrue(listener.response.isPresent());
        assertEquals(12_345L, listener.response.get().longValue());
        assertEquals(2, clientUtil.configCallCount);
        assertSourceAccessRequestTargetsLogs();
    }

    public void testExecuteMinDataTimeQueryUsesUserSecurityWhenConfigHasUser() {
        User user = new User("alice", Collections.emptyList(), Collections.singletonList("role"), Collections.emptyList());
        when(config.getUser()).thenReturn(user);
        clientUtil.response = new RawPPLActionResponse("{\"datarows\":[[12345]]}");
        CapturingListener<Optional<Long>> listener = new CapturingListener<>();

        executor.executeMinDataTimeQuery(config, AnalysisType.AD, listener);

        assertNull(listener.failure);
        assertTrue(listener.response.isPresent());
        assertEquals(2, clientUtil.userCallCount);
        assertEquals(user, clientUtil.capturedUser);
        assertEquals(0, clientUtil.configCallCount);
        assertSourceAccessRequestTargetsLogs();
    }

    public void testExecuteDateRangeQueryParsesTextAndNumericTimestamps() {
        clientUtil.response = new RawPPLActionResponse("{\"datarows\":[[\"1970-01-01 00:00:01\",2000]]}");
        CapturingListener<Pair<Long, Long>> listener = new CapturingListener<>();

        executor.executeDateRangeQuery(null, config, AnalysisType.AD, listener);

        assertNull(listener.failure);
        assertEquals(1_000L, listener.response.getLeft().longValue());
        assertEquals(2_000L, listener.response.getRight().longValue());
    }

    public void testExecuteDateRangeQueryFailsWhenTimestampMissing() {
        clientUtil.response = new RawPPLActionResponse("{\"datarows\":[[1000]]}");
        CapturingListener<Pair<Long, Long>> listener = new CapturingListener<>();

        executor.executeDateRangeQuery(null, config, AnalysisType.AD, listener);

        assertNotNull(listener.failure);
        assertTrue(listener.failure instanceof IllegalStateException);
        assertTrue(listener.failure.getMessage().contains("did not return both min and max"));
    }

    public void testExecuteQueryRejectsMissingPPLSource() {
        when(config.getPPLSource()).thenReturn(null);
        CapturingListener<Optional<Long>> listener = new CapturingListener<>();

        executor.executeMinDataTimeQuery(config, AnalysisType.AD, listener);

        assertNotNull(listener.failure);
        assertTrue(listener.failure instanceof IllegalArgumentException);
        assertTrue(listener.failure.getMessage().contains("ppl_source must be set"));
    }

    public void testExecuteQueryPropagatesSecurityFailure() {
        clientUtil.failure = new IllegalStateException("security failed");
        CapturingListener<Optional<Long>> listener = new CapturingListener<>();

        executor.executeMinDataTimeQuery(config, AnalysisType.AD, listener);

        assertSame(clientUtil.failure, listener.failure);
    }

    public void testPPLTransportResponseFromActionResponseSupportsSerializedAndDirectResponses() throws Exception {
        Method fromActionResponse = pplTransportResponseClass().getDeclaredMethod("fromActionResponse", ActionResponse.class);
        fromActionResponse.setAccessible(true);
        Method getResult = pplTransportResponseClass().getDeclaredMethod("getResult");
        getResult.setAccessible(true);

        Object serializedResponse = fromActionResponse.invoke(null, new RawPPLActionResponse("{\"datarows\":[[1]]}"));
        assertEquals("{\"datarows\":[[1]]}", getResult.invoke(serializedResponse));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (OutputStreamStreamOutput output = new OutputStreamStreamOutput(baos)) {
            ((ActionResponse) serializedResponse).writeTo(output);
        }
        try (InputStreamStreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
            Object roundTrippedResponse = pplTransportResponseConstructor().newInstance(input);
            assertEquals("{\"datarows\":[[1]]}", getResult.invoke(roundTrippedResponse));
        }

        Object directResponse = fromActionResponse.invoke(null, serializedResponse);
        assertSame(serializedResponse, directResponse);
    }

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

    public void testPPLTransportResponseFromActionResponseFailsOnInvalidSerializedResponse() throws Exception {
        Method fromActionResponse = pplTransportResponseClass().getDeclaredMethod("fromActionResponse", ActionResponse.class);
        fromActionResponse.setAccessible(true);

        InvocationTargetException exception = expectThrows(
            InvocationTargetException.class,
            () -> fromActionResponse.invoke(null, new FailingActionResponse())
        );
        assertTrue(exception.getCause() instanceof IllegalStateException);
        assertTrue(exception.getCause().getMessage().contains("failed to parse ActionResponse"));
    }

    private static Constructor<?> pplTransportResponseConstructor() throws Exception {
        Constructor<?> constructor = pplTransportResponseClass().getDeclaredConstructor(StreamInput.class);
        constructor.setAccessible(true);
        return constructor;
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

    private static ActionRequest newPPLTransportRequest(String query, String format, String path) throws Exception {
        return (ActionRequest) pplTransportRequestConstructor().newInstance(query, format, path);
    }

    private static Class<?> pplTransportResponseClass() throws ClassNotFoundException {
        return Class.forName("org.opensearch.timeseries.feature.PPLDirectQueryExecutor$PPLTransportResponse");
    }

    private static Class<?> pplTransportRequestClass() throws ClassNotFoundException {
        return Class.forName("org.opensearch.timeseries.feature.PPLDirectQueryExecutor$PPLTransportRequest");
    }

    private static void assertSerializedRequestContains(ActionRequest request, String expectedQueryFragment) throws Exception {
        assertNotNull(request);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (OutputStreamStreamOutput output = new OutputStreamStreamOutput(baos)) {
            request.writeTo(output);
        }

        String query = (String) readPrivateField(request, "query");
        assertTrue(query, query.contains(expectedQueryFragment));
        assertEquals("jdbc", readPrivateField(request, "format"));
        assertNull(readPrivateField(request, "explainMode"));
        assertNull(readPrivateField(request, "jsonContent"));
        assertEquals("/_plugins/_ppl", readPrivateField(request, "path"));
        assertTrue((Boolean) readPrivateField(request, "sanitize"));
    }

    private void assertSourceAccessRequestTargetsLogs() {
        assertNotNull(clientUtil.capturedSourceAccessRequest);
        assertArrayEquals(new String[] { "logs" }, clientUtil.capturedSourceAccessRequest.indices());
        assertEquals(0, clientUtil.capturedSourceAccessRequest.source().size());
    }

    private static Object readPrivateField(Object target, String fieldName) throws Exception {
        java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static class CapturingListener<T> implements ActionListener<T> {
        private T response;
        private Exception failure;

        @Override
        public void onResponse(T response) {
            this.response = response;
        }

        @Override
        public void onFailure(Exception e) {
            this.failure = e;
        }
    }

    private static class RawPPLActionResponse extends ActionResponse {
        private final String result;

        private RawPPLActionResponse(String result) {
            this.result = result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(result);
            out.writeString("application/json");
        }
    }

    private static class FailingActionResponse extends ActionResponse {
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new IOException("cannot serialize");
        }
    }

    private static class TestSecurityClientUtil extends SecurityClientUtil {
        private ActionResponse response;
        private Exception failure;
        private ActionRequest capturedRequest;
        private SearchRequest capturedSourceAccessRequest;
        private String capturedConfigId;
        private User capturedUser;
        private int configCallCount;
        private int userCallCount;

        private TestSecurityClientUtil() {
            super(mock(NodeStateManager.class), Settings.EMPTY);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> void asyncRequestWithInjectedSecurity(
            Request request,
            BiConsumer<Request, ActionListener<Response>> consumer,
            String configId,
            Client client,
            AnalysisType context,
            ActionListener<Response> listener
        ) {
            capturedRequest = request;
            capturedConfigId = configId;
            configCallCount++;
            respond(request, listener);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> void asyncRequestWithInjectedSecurity(
            Request request,
            BiConsumer<Request, ActionListener<Response>> consumer,
            User user,
            Client client,
            AnalysisType context,
            ActionListener<Response> listener
        ) {
            capturedRequest = request;
            capturedUser = user;
            userCallCount++;
            respond(request, listener);
        }

        @SuppressWarnings("unchecked")
        private <Request extends ActionRequest, Response extends ActionResponse> void respond(
            Request request,
            ActionListener<Response> listener
        ) {
            if (failure != null) {
                listener.onFailure(failure);
            } else if (request instanceof SearchRequest) {
                capturedSourceAccessRequest = (SearchRequest) request;
                listener.onResponse((Response) mock(SearchResponse.class));
            } else {
                listener.onResponse((Response) response);
            }
        }
    }
}
