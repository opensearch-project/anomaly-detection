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

package org.opensearch.timeseries.model;

import org.opensearch.test.OpenSearchTestCase;

public class PPLSourceTests extends OpenSearchTestCase {

    public void testCompileSupportsCountAndPreservesPreStatsPipeline() {
        PPLSource.CompiledPPLQuery compiledQuery = PPLSource
            .compile(
                "source = sample-http-responses | eval error_code = status_code | where error_code >= 400 | stats count() as error_count, sum(http_5xx) as sum_http_5xx by span(timestamp, 60m) as bucket | sort bucket asc"
            );

        assertEquals("sample-http-responses", compiledQuery.getIndex());
        assertEquals(2, compiledQuery.getPreStatsStages().size());
        assertEquals("eval error_code = status_code", compiledQuery.getPreStatsStages().get(0));
        assertEquals("where error_code >= 400", compiledQuery.getPreStatsStages().get(1));
        assertEquals("timestamp", compiledQuery.getTimeField());
        assertEquals(2, compiledQuery.getMetricCount());
        assertEquals("error_count", compiledQuery.getFeatureNames().get(0));
        assertEquals("sum_http_5xx", compiledQuery.getFeatureNames().get(1));
        assertEquals(60L, compiledQuery.getInterval().getInterval());
        assertEquals(java.time.temporal.ChronoUnit.MINUTES, compiledQuery.getInterval().getUnit());

        String metricQuery = compiledQuery.buildMetricQueryForRange(1_000L, 2_000L);
        assertTrue(metricQuery.startsWith("source = sample-http-responses | eval error_code = status_code | where error_code >= 400"));
        assertTrue(metricQuery.contains("| where timestamp >= \"1970-01-01 00:00:01.000\" and timestamp < \"1970-01-01 00:00:02.000\""));
        assertTrue(metricQuery.endsWith("| stats count() as error_count, sum(http_5xx) as sum_http_5xx"));
    }

    public void testCompileSupportsCountStarAndDerivedMetricExpressions() {
        PPLSource.CompiledPPLQuery compiledQuery = PPLSource
            .compile(
                "source = logs-* | where status >= 400 | stats count(*) as doc_count, avg(bytes / 1024) as avg_kb by span(`event.time`, 10m) as bucket"
            );

        assertEquals("logs-*", compiledQuery.getIndex());
        assertEquals("event.time", compiledQuery.getTimeField());
        assertEquals(2, compiledQuery.getMetricCount());
        assertEquals("doc_count", compiledQuery.getFeatureNames().get(0));
        assertEquals("avg_kb", compiledQuery.getFeatureNames().get(1));

        String metricQuery = compiledQuery.buildMetricQueryForRange(60_000L, 120_000L);
        assertTrue(metricQuery.contains("| stats count() as doc_count, avg(bytes / 1024) as avg_kb"));
        assertTrue(metricQuery.contains("event.time >= \"1970-01-01 00:01:00.000\" and event.time < \"1970-01-01 00:02:00.000\""));
    }

    public void testCompileSupportsCountFieldAndQuotedIdentifiers() {
        PPLSource.CompiledPPLQuery compiledQuery = PPLSource
            .compile(
                "source = `logs-2026` | where `service.name` = 'checkout' | stats count(`error.code`) as error_code_count by span(`@timestamp`, 5m)"
            );

        assertEquals("logs-2026", compiledQuery.getIndex());
        assertEquals("@timestamp", compiledQuery.getTimeField());
        assertEquals(1, compiledQuery.getMetricCount());
        assertEquals("error_code_count", compiledQuery.getFeatureNames().get(0));

        String metricQuery = compiledQuery.buildMetricQueryForRange(300_000L, 600_000L);
        assertTrue(metricQuery.contains("| stats count(`error.code`) as error_code_count"));
        assertTrue(metricQuery.contains("`@timestamp` >= \"1970-01-01 00:05:00.000\" and `@timestamp` < \"1970-01-01 00:10:00.000\""));
    }

    public void testCompilePreservesDerivedTimeFieldForAuxiliaryQueries() {
        PPLSource.CompiledPPLQuery compiledQuery = PPLSource
            .compile("source = logs | eval bucket_time = @timestamp | stats count() as error_count by span(bucket_time, 10m)");

        assertEquals(
            "source = logs | eval bucket_time = @timestamp | stats max(bucket_time) as latest_time",
            compiledQuery.buildLatestTimeQuery()
        );
        assertEquals(
            "source = logs | eval bucket_time = @timestamp | stats min(bucket_time) as min_time",
            compiledQuery.buildMinTimeQuery()
        );
        assertEquals(
            "source = logs | eval bucket_time = @timestamp | stats min(bucket_time) as min_time, max(bucket_time) as max_time",
            compiledQuery.buildDateRangeQuery()
        );
    }

    public void testCompileRejectsDuplicateMetricAliases() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> PPLSource.compile("source = logs | stats count() as dup, sum(bytes) as dup by span(timestamp, 10m)")
        );
        assertTrue(exception.getMessage().contains("Duplicate metric alias [dup]"));
    }

    public void testCompileRejectsNonSortStageAfterFinalStats() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> PPLSource.compile("source = logs | stats count() as error_count by span(timestamp, 10m) | head 5")
        );
        assertTrue(exception.getMessage().contains("after the final stats stage"));
    }

    public void testCompileRejectsUnsupportedPreStatsStage() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> PPLSource.compile("source = logs | head 5 | stats count() as error_count by span(timestamp, 10m)")
        );
        assertTrue(exception.getMessage().contains("before the final stats stage"));
        assertTrue(exception.getMessage().contains("where and eval"));
    }
}
