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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.PrometheusSource;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.client.Client;

public class CompositeRetrieverPrometheusTests extends OpenSearchTestCase {

    @SuppressWarnings("unchecked")
    public void testPrometheusIteratorReturnsEntityPageAndClosesExecutor() throws Exception {
        Config config = mockPrometheusConfig(Arrays.asList("service"));
        PrometheusDirectQueryExecutor prometheusExecutor = mock(PrometheusDirectQueryExecutor.class);
        Map<Map<String, String>, NavigableMap<Long, Double>> valuesBySeries = new LinkedHashMap<>();
        valuesBySeries.put(Map.of("service", "checkout"), values(2_000L, 5.0d));
        valuesBySeries.put(Map.of("service", "payments"), values(500L, 9.0d));
        valuesBySeries.put(Map.of("host", "missing-service-label"), values(2_000L, 7.0d));
        valuesBySeries.put(Map.of("service", "empty-values"), new TreeMap<>());
        valuesBySeries.put(Map.of("service", "null-value"), values(2_000L, null));
        valuesBySeries.put(Map.of("service", "null-map"), null);

        doAnswer(invocation -> {
            ActionListener<Map<Map<String, String>, NavigableMap<Long, Double>>> listener = invocation.getArgument(5);
            listener.onResponse(valuesBySeries);
            return null;
        })
            .when(prometheusExecutor)
            .executeRangeQueryBySeries(eq(config), eq(1_000L), eq(2_000L), eq(60L), eq(AnalysisType.AD), any(ActionListener.class));

        CompositeRetriever.PageIterator iterator = newRetriever(config, prometheusExecutor, 1_000L, 2_000L).iterator();
        assertTrue(iterator.hasNext());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<CompositeRetriever.Page> page = new AtomicReference<>();
        iterator.next(ActionListener.wrap(value -> {
            page.set(value);
            latch.countDown();
        }, e -> { throw new AssertionError(e); }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertFalse(iterator.hasNext());
        assertTrue(iterator.toString().contains("CompositeRetriever"));
        assertEquals(1, page.get().getResults().size());
        Entity entity = page.get().getResults().keySet().iterator().next();
        assertEquals("checkout", entity.getAttributes().get("service"));
        assertEquals(5.0d, page.get().getResults().get(entity)[0], 0.001d);
        verify(prometheusExecutor).close();
    }

    @SuppressWarnings("unchecked")
    public void testPrometheusIteratorFailureClosesExecutor() throws Exception {
        Config config = mockPrometheusConfig(Arrays.asList("service"));
        PrometheusDirectQueryExecutor prometheusExecutor = mock(PrometheusDirectQueryExecutor.class);
        RuntimeException failure = new RuntimeException("boom");
        doAnswer(invocation -> {
            ActionListener<Map<Map<String, String>, NavigableMap<Long, Double>>> listener = invocation.getArgument(5);
            listener.onFailure(failure);
            return null;
        })
            .when(prometheusExecutor)
            .executeRangeQueryBySeries(eq(config), eq(1_000L), eq(2_000L), eq(60L), eq(AnalysisType.AD), any(ActionListener.class));

        CompositeRetriever.PageIterator iterator = newRetriever(config, prometheusExecutor, 1_000L, 2_000L).iterator();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> receivedFailure = new AtomicReference<>();

        iterator.next(ActionListener.wrap(value -> fail("Expected Prometheus iterator failure"), e -> {
            receivedFailure.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertSame(failure, receivedFailure.get());
        verify(prometheusExecutor).close();
    }

    @SuppressWarnings("unchecked")
    public void testPrometheusIteratorWithEmptyCategoryFieldsReturnsEmptyPage() throws Exception {
        Config config = mockPrometheusConfig(java.util.Collections.emptyList());
        PrometheusDirectQueryExecutor prometheusExecutor = mock(PrometheusDirectQueryExecutor.class);
        Map<Map<String, String>, NavigableMap<Long, Double>> valuesBySeries = Map.of(Map.of("service", "checkout"), values(2_000L, 5.0d));
        doAnswer(invocation -> {
            ActionListener<Map<Map<String, String>, NavigableMap<Long, Double>>> listener = invocation.getArgument(5);
            listener.onResponse(valuesBySeries);
            return null;
        })
            .when(prometheusExecutor)
            .executeRangeQueryBySeries(eq(config), eq(1_000L), eq(2_000L), eq(60L), eq(AnalysisType.AD), any(ActionListener.class));

        CompositeRetriever.PageIterator iterator = newRetriever(config, prometheusExecutor, 1_000L, 2_000L).iterator();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<CompositeRetriever.Page> page = new AtomicReference<>();

        iterator.next(ActionListener.wrap(value -> {
            page.set(value);
            latch.countDown();
        }, e -> { throw new AssertionError(e); }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(page.get().isEmpty());
    }

    @SuppressWarnings("unchecked")
    public void testPrometheusIteratorWithInvalidRangeReturnsEmptyPage() throws Exception {
        Config config = mockPrometheusConfig(Arrays.asList("service"));
        PrometheusDirectQueryExecutor prometheusExecutor = mock(PrometheusDirectQueryExecutor.class);
        Map<Map<String, String>, NavigableMap<Long, Double>> valuesBySeries = Map.of(Map.of("service", "checkout"), values(2_000L, 5.0d));
        doAnswer(invocation -> {
            ActionListener<Map<Map<String, String>, NavigableMap<Long, Double>>> listener = invocation.getArgument(5);
            listener.onResponse(valuesBySeries);
            return null;
        })
            .when(prometheusExecutor)
            .executeRangeQueryBySeries(eq(config), eq(2_000L), eq(2_000L), eq(60L), eq(AnalysisType.AD), any(ActionListener.class));

        CompositeRetriever.PageIterator iterator = newRetriever(config, prometheusExecutor, 2_000L, 2_000L).iterator();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<CompositeRetriever.Page> page = new AtomicReference<>();

        iterator.next(ActionListener.wrap(value -> {
            page.set(value);
            latch.countDown();
        }, e -> { throw new AssertionError(e); }));

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(page.get().isEmpty());
    }

    private CompositeRetriever newRetriever(
        Config config,
        PrometheusDirectQueryExecutor prometheusExecutor,
        long startTimeMs,
        long endTimeMs
    ) {
        return new CompositeRetriever(
            startTimeMs,
            endTimeMs,
            config,
            mock(NamedXContentRegistry.class),
            mock(Client.class),
            mock(SecurityClientUtil.class),
            10_000L,
            Clock.fixed(Instant.EPOCH, ZoneOffset.UTC),
            Settings.EMPTY,
            100,
            10,
            mock(IndexNameExpressionResolver.class),
            mock(ClusterService.class),
            AnalysisType.AD,
            prometheusExecutor
        );
    }

    private Config mockPrometheusConfig(java.util.List<String> categoryFields) {
        Config config = mock(Config.class);
        when(config.getSourceType()).thenReturn(Config.SOURCE_TYPE_PROMETHEUS);
        when(config.getPrometheusSource()).thenReturn(mock(PrometheusSource.class));
        when(config.getCategoryFields()).thenReturn(categoryFields);
        when(config.getIntervalInSeconds()).thenReturn(60L);
        return config;
    }

    private NavigableMap<Long, Double> values(long timestamp, Double value) {
        NavigableMap<Long, Double> values = new TreeMap<>();
        values.put(timestamp, value);
        return values;
    }
}
