/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ad.ml;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.opensearch.ad.correlation.Anomaly;
import org.opensearch.ad.correlation.AnomalyCorrelation;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectorMetadata;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.model.Entity;

public class InsightsGeneratorTests extends OpenSearchTestCase {

    @SuppressWarnings("unchecked")
    @Test
    public void testGenerateInsightsFromClustersBasic() throws Exception {
        Instant windowStart = Instant.now().minus(2, ChronoUnit.HOURS);
        Instant windowEnd = Instant.now().minus(1, ChronoUnit.HOURS);

        Instant a1Start = windowStart.plus(5, ChronoUnit.MINUTES);
        Instant a1End = a1Start.plus(10, ChronoUnit.MINUTES);
        Instant a2Start = windowStart.plus(15, ChronoUnit.MINUTES);
        Instant a2End = a2Start.plus(5, ChronoUnit.MINUTES);

        Anomaly a1 = new Anomaly("m1", "detector-1", a1Start, a1End);
        Anomaly a2 = new Anomaly("m2", "detector-2", a2Start, a2End);
        AnomalyCorrelation.Cluster cluster = new AnomalyCorrelation.Cluster(
            new AnomalyCorrelation.EventWindow(windowStart, windowEnd),
            List.of(a1, a2)
        );

        // Provide entity info for one anomaly to cover entityKey path
        AnomalyResult r1 = mock(AnomalyResult.class);
        when(r1.getEntity()).thenReturn(java.util.Optional.of(Entity.createSingleAttributeEntity("host", "server-1")));

        Map<Anomaly, AnomalyResult> anomalyResultByAnomaly = Map.of(a1, r1);
        Map<String, DetectorMetadata> detectorMetadata = Map
            .of(
                "detector-1",
                new DetectorMetadata("detector-1", "Detector One", List.of("index-1")),
                "detector-2",
                new DetectorMetadata("detector-2", "Detector Two", List.of("index-2"))
            );

        var builderOpt = InsightsGenerator
            .generateInsightsFromClusters(List.of(cluster), anomalyResultByAnomaly, detectorMetadata, windowStart, windowEnd);
        assertTrue(builderOpt.isPresent());
        var builder = builderOpt.get();

        Map<String, Object> asMap = XContentHelper.convertToMap(new BytesArray(builder.toString()), false, XContentType.JSON).v2();

        assertEquals(windowStart.toEpochMilli(), ((Number) asMap.get("window_start")).longValue());
        assertEquals(windowEnd.toEpochMilli(), ((Number) asMap.get("window_end")).longValue());
        assertNotNull(asMap.get("generated_at"));

        // task_id was removed from insights docs; _id can be used if needed
        assertFalse(asMap.containsKey("task_id"));

        List<String> docDetectorIds = (List<String>) asMap.get("doc_detector_ids");
        assertTrue(docDetectorIds.contains("detector-1"));
        assertTrue(docDetectorIds.contains("detector-2"));

        List<Map<String, Object>> clusters = (List<Map<String, Object>>) asMap.get("clusters");
        assertEquals(1, clusters.size());

        Map<String, Object> stats = (Map<String, Object>) asMap.get("stats");
        assertEquals(1, ((Number) stats.get("num_clusters")).intValue());
        assertEquals(2, ((Number) stats.get("num_anomalies")).intValue());
        assertEquals(2, ((Number) stats.get("num_detectors")).intValue());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGenerateInsightsFromClustersNullInputs() throws Exception {
        Instant windowStart = Instant.now().minus(2, ChronoUnit.HOURS);
        Instant windowEnd = Instant.now().minus(1, ChronoUnit.HOURS);

        var builderOpt = InsightsGenerator.generateInsightsFromClusters(null, null, null, windowStart, windowEnd);
        assertTrue(builderOpt.isEmpty());
    }
}
