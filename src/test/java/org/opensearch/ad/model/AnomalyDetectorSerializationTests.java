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

package org.opensearch.ad.model;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Locale;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class AnomalyDetectorSerializationTests extends OpenSearchSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, TimeSeriesAnalyticsPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    public void testDetectorWithUiMetadata() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableMap.of("testKey", "testValue"), Instant.now());
        BytesStreamOutput output = new BytesStreamOutput();
        detector.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyDetector parsedDetector = new AnomalyDetector(input);
        assertTrue(parsedDetector.equals(detector));
    }

    public void testDetectorWithoutUiMetadata() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(null, Instant.now());
        BytesStreamOutput output = new BytesStreamOutput();
        detector.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyDetector parsedDetector = new AnomalyDetector(input);
        assertTrue(parsedDetector.equals(detector));
    }

    public void testHCDetector() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields("testId", ImmutableList.of("category_field"));
        BytesStreamOutput output = new BytesStreamOutput();
        detector.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyDetector parsedDetector = new AnomalyDetector(input);
        assertTrue(parsedDetector.equals(detector));
    }

    public void testWithoutUser() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields("testId", ImmutableList.of("category_field"));
        detector.setUser(null);
        BytesStreamOutput output = new BytesStreamOutput();
        detector.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyDetector parsedDetector = new AnomalyDetector(input);
        assertTrue(String.format(Locale.ROOT, "expected %s, but got %s", detector, parsedDetector), detector.equals(parsedDetector));
    }

    public void testPrometheusSourceSerializationRoundTrip() throws IOException {
        String detectorString = "{"
            + "\"name\":\"prom-detector\","
            + "\"description\":\"prometheus detector\","
            + "\"source_type\":\"PROMETHEUS\","
            + "\"prometheus_source\":{"
            + "\"query_language\":\"PROMQL\","
            + "\"query\":\"rate(go_gc_heap_allocs_bytes_total{instance=\\\"localhost:9090\\\"}[5m])\","
            + "\"data_connection_id\":\"prome\","
            + "\"series_filter\":{\"instance\":\"localhost:9090\"}"
            + "},"
            + "\"feature_attributes\":[{"
            + "\"feature_id\":\"f1\","
            + "\"feature_name\":\"prom_value\","
            + "\"feature_enabled\":true,"
            + "\"aggregation_query\":{\"f1\":{\"avg\":{\"field\":\"value\"}}}"
            + "}],"
            + "\"detection_interval\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},"
            + "\"window_delay\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},"
            + "\"last_update_time\":1700000000000"
            + "}";

        AnomalyDetector detector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        BytesStreamOutput output = new BytesStreamOutput();
        detector.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyDetector parsedDetector = new AnomalyDetector(input);

        assertEquals(detector, parsedDetector);
        assertEquals(Config.SOURCE_TYPE_PROMETHEUS, parsedDetector.getSourceType());
        assertNotNull(parsedDetector.getPrometheusSource());
        assertEquals("prome", parsedDetector.getPrometheusSource().getDataConnectionId());
        assertEquals("localhost:9090", parsedDetector.getPrometheusSource().getSeriesFilter().get("instance"));
    }

    public void testPPLSourceSerializationRoundTrip() throws IOException {
        String detectorString = "{"
            + "\"name\":\"ppl-detector\","
            + "\"description\":\"ppl detector\","
            + "\"source_type\":\"PPL\","
            + "\"ppl_source\":{"
            + "\"query_language\":\"PPL\","
            + "\"query\":\"source = sample-http-responses | stats sum(http_4xx) as sum_http_4xx by span(timestamp, 10m) as bucket\""
            + "},"
            + "\"window_delay\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},"
            + "\"last_update_time\":1700000000000"
            + "}";

        AnomalyDetector detector = AnomalyDetector.parse(TestHelpers.parser(detectorString));
        BytesStreamOutput output = new BytesStreamOutput();
        detector.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        AnomalyDetector parsedDetector = new AnomalyDetector(input);

        assertEquals(detector, parsedDetector);
        assertEquals(Config.SOURCE_TYPE_PPL, parsedDetector.getSourceType());
        assertNotNull(parsedDetector.getPPLSource());
        assertEquals("PPL", parsedDetector.getPPLSource().getQueryLanguage());
        assertEquals(
            "source = sample-http-responses | stats sum(http_4xx) as sum_http_4xx by span(timestamp, 10m) as bucket",
            parsedDetector.getPPLSource().getQuery()
        );
        assertEquals("timestamp", parsedDetector.getTimeField());
        assertEquals(10L, ((IntervalTimeConfiguration) parsedDetector.getInterval()).getInterval());
    }

}
