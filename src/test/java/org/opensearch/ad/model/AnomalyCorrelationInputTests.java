/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.ad.model;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.opensearch.ad.correlation.Anomaly;
import org.opensearch.test.OpenSearchTestCase;

public class AnomalyCorrelationInputTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        List<Anomaly> anomalies = Arrays
            .asList(
                new Anomaly("model-1", "detector-1", Instant.parse("2025-01-01T00:00:00Z"), Instant.parse("2025-01-01T00:01:00Z")),
                new Anomaly("model-2", "detector-2", Instant.parse("2025-01-01T00:02:00Z"), Instant.parse("2025-01-01T00:03:00Z"))
            );
        List<AnomalyDetector> detectors = Collections.emptyList();

        AnomalyCorrelationInput input = new AnomalyCorrelationInput(anomalies, detectors);

        assertEquals(anomalies, input.getAnomalies());
        assertEquals(detectors, input.getDetectors());
        assertEquals(2, input.getAnomalyCount());
        assertEquals(0, input.getDetectorCount());
    }
}
