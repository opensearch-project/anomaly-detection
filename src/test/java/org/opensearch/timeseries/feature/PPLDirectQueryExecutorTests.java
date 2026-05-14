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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.action.ActionRequest;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.PPLSource;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.client.Client;

public class PPLDirectQueryExecutorTests extends OpenSearchTestCase {

    public void testExecuteMetricQueryParsesNumericAndTextValues() throws Exception {
        PPLDirectQueryExecutor executor = newExecutorReturning("{\"datarows\":[[42,\"7.5\"]]}");
        Config config = mockPPLConfig("source = logs | stats count() as error_count, sum(bytes) as sum_bytes by span(timestamp, 1m)");

        Optional<double[]> result = executeMetric(executor, config, 1_000L, 2_000L);

        assertTrue(result.isPresent());
        assertEquals(42.0d, result.get()[0], 0.001d);
        assertEquals(7.5d, result.get()[1], 0.001d);
    }

    public void testExecuteMetricQueryReturnsEmptyWhenRowsAreMissingOrNull() throws Exception {
        PPLDirectQueryExecutor emptyRowsExecutor = newExecutorReturning("{\"datarows\":[]}");
        Config config = mockPPLConfig("source = logs | stats count() as error_count by span(timestamp, 1m)");
        assertFalse(executeMetric(emptyRowsExecutor, config, 1_000L, 2_000L).isPresent());

        PPLDirectQueryExecutor nullMetricExecutor = newExecutorReturning("{\"datarows\":[[null]]}");
        assertFalse(executeMetric(nullMetricExecutor, config, 1_000L, 2_000L).isPresent());
    }

    public void testExecuteLatestDataTimeQueryParsesTextTimestampWithUser() throws Exception {
        PPLDirectQueryExecutor executor = newExecutorReturning("{\"datarows\":[[\"2026-05-13 12:34:56.789\"]]}");
        Config config = mockPPLConfig("source = logs | stats count() as error_count by span(timestamp, 1m)");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Optional<Long>> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        User user = new User("test-user", Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

        executor.executeLatestDataTimeQuery(user, config, AnalysisType.AD, ActionListener.wrap(value -> {
            result.set(value);
            latch.countDown();
        }, e -> {
            failure.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNull(failure.get());
        assertTrue(result.get().isPresent());
        assertEquals(
            LocalDateTime.of(2026, 5, 13, 12, 34, 56, 789_000_000).toInstant(ZoneOffset.UTC).toEpochMilli(),
            result.get().get().longValue()
        );
    }

    public void testExecuteMinDataTimeQueryParsesNumericTimestamp() throws Exception {
        PPLDirectQueryExecutor executor = newExecutorReturning("{\"datarows\":[[1700000000000]]}");
        Config config = mockPPLConfig("source = logs | stats count() as error_count by span(timestamp, 1m)");
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Optional<Long>> result = new AtomicReference<>();

        executor.executeMinDataTimeQuery(config, AnalysisType.AD, ActionListener.wrap(value -> {
            result.set(value);
            latch.countDown();
        }, e -> { throw new AssertionError(e); }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(result.get().isPresent());
        assertEquals(1700000000000L, result.get().get().longValue());
    }

    public void testExecuteDateRangeQueryParsesRangeAndFailsWhenIncomplete() throws Exception {
        Config config = mockPPLConfig("source = logs | stats count() as error_count by span(timestamp, 1m)");
        PPLDirectQueryExecutor successExecutor = newExecutorReturning("{\"datarows\":[[1000,2000]]}");
        CountDownLatch successLatch = new CountDownLatch(1);
        AtomicReference<Pair<Long, Long>> range = new AtomicReference<>();

        successExecutor.executeDateRangeQuery(null, config, AnalysisType.AD, ActionListener.wrap(value -> {
            range.set(value);
            successLatch.countDown();
        }, e -> { throw new AssertionError(e); }));

        assertTrue(successLatch.await(5, TimeUnit.SECONDS));
        assertEquals(1000L, range.get().getLeft().longValue());
        assertEquals(2000L, range.get().getRight().longValue());

        PPLDirectQueryExecutor failureExecutor = newExecutorReturning("{\"datarows\":[[1000]]}");
        CountDownLatch failureLatch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        failureExecutor
            .executeDateRangeQuery(
                null,
                config,
                AnalysisType.AD,
                ActionListener.wrap(value -> fail("Expected incomplete date range to fail"), e -> {
                    failure.set(e);
                    failureLatch.countDown();
                })
            );

        assertTrue(failureLatch.await(5, TimeUnit.SECONDS));
        assertTrue(failure.get() instanceof IllegalStateException);
    }

    public void testExecuteQueryFailsFastForMissingOrInvalidPPLSource() throws Exception {
        PPLDirectQueryExecutor executor = newExecutorReturning("{\"datarows\":[[1]]}");
        CountDownLatch missingLatch = new CountDownLatch(1);
        AtomicReference<Exception> missingFailure = new AtomicReference<>();

        executor
            .executeLatestDataTimeQuery(
                null,
                mock(Config.class),
                AnalysisType.AD,
                ActionListener.wrap(value -> fail("Expected missing ppl_source to fail"), e -> {
                    missingFailure.set(e);
                    missingLatch.countDown();
                })
            );

        assertTrue(missingLatch.await(5, TimeUnit.SECONDS));
        assertTrue(missingFailure.get().getMessage().contains("ppl_source must be set"));

        CountDownLatch invalidLatch = new CountDownLatch(1);
        AtomicReference<Exception> invalidFailure = new AtomicReference<>();
        Config invalidConfig = mockPPLConfig("source = logs | head 1 | stats count() as c by span(timestamp, 1m)");
        executor
            .executeMetricQuery(
                invalidConfig,
                1L,
                2L,
                AnalysisType.AD,
                ActionListener.wrap(value -> fail("Expected invalid PPL to fail"), e -> {
                    invalidFailure.set(e);
                    invalidLatch.countDown();
                })
            );

        assertTrue(invalidLatch.await(5, TimeUnit.SECONDS));
        assertTrue(invalidFailure.get() instanceof IllegalArgumentException);
    }

    public void testTransportResponseRoundTripViaReflection() throws Exception {
        Class<?> responseClass = Class.forName("org.opensearch.timeseries.feature.PPLDirectQueryExecutor$PPLTransportResponse");
        Method fromActionResponse = responseClass.getDeclaredMethod("fromActionResponse", ActionResponse.class);
        fromActionResponse.setAccessible(true);
        Object response = fromActionResponse.invoke(null, new TestPPLActionResponse("{\"datarows\":[[1]]}"));

        Method getResult = responseClass.getDeclaredMethod("getResult");
        getResult.setAccessible(true);
        assertEquals("{\"datarows\":[[1]]}", getResult.invoke(response));
        assertSame(response, fromActionResponse.invoke(null, response));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (OutputStreamStreamOutput output = new OutputStreamStreamOutput(baos)) {
            ((ActionResponse) response).writeTo(output);
        }

        Constructor<?> constructor = responseClass.getDeclaredConstructor(StreamInput.class);
        constructor.setAccessible(true);
        Object parsed;
        try (InputStreamStreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
            parsed = constructor.newInstance(input);
        }
        assertEquals("{\"datarows\":[[1]]}", getResult.invoke(parsed));
    }

    private PPLDirectQueryExecutor newExecutorReturning(String responseBody) {
        Client client = mock(Client.class);
        SecurityClientUtil clientUtil = mock(SecurityClientUtil.class);
        stubConfigIdSecurityResponse(clientUtil, responseBody);
        stubUserSecurityResponse(clientUtil, responseBody);
        return new PPLDirectQueryExecutor(client, clientUtil);
    }

    @SuppressWarnings("unchecked")
    private void stubConfigIdSecurityResponse(SecurityClientUtil clientUtil, String responseBody) {
        doAnswer(invocation -> {
            ActionListener<ActionResponse> listener = invocation.getArgument(5);
            listener.onResponse(new TestPPLActionResponse(responseBody));
            return null;
        })
            .when(clientUtil)
            .asyncRequestWithInjectedSecurity(
                any(ActionRequest.class),
                any(),
                anyString(),
                any(Client.class),
                any(AnalysisType.class),
                any(ActionListener.class)
            );
    }

    @SuppressWarnings("unchecked")
    private void stubUserSecurityResponse(SecurityClientUtil clientUtil, String responseBody) {
        doAnswer(invocation -> {
            ActionListener<ActionResponse> listener = invocation.getArgument(5);
            listener.onResponse(new TestPPLActionResponse(responseBody));
            return null;
        })
            .when(clientUtil)
            .asyncRequestWithInjectedSecurity(
                any(ActionRequest.class),
                any(),
                any(User.class),
                any(Client.class),
                any(AnalysisType.class),
                any(ActionListener.class)
            );
    }

    private Config mockPPLConfig(String query) {
        Config config = mock(Config.class);
        when(config.getId()).thenReturn("detector-1");
        when(config.getPPLSource()).thenReturn(new PPLSource("PPL", query));
        return config;
    }

    private Optional<double[]> executeMetric(PPLDirectQueryExecutor executor, Config config, long startTimeMs, long endTimeMs)
        throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Optional<double[]>> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();

        executor.executeMetricQuery(config, startTimeMs, endTimeMs, AnalysisType.AD, ActionListener.wrap(value -> {
            result.set(value);
            latch.countDown();
        }, e -> {
            failure.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNull(failure.get());
        return result.get();
    }

    private static class TestPPLActionResponse extends ActionResponse {
        private final String responseBody;

        private TestPPLActionResponse(String responseBody) {
            this.responseBody = responseBody;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(responseBody);
            out.writeString("application/json");
        }
    }
}
