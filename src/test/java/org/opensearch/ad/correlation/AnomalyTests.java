/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *
 */
package org.opensearch.ad.correlation;

import java.time.Duration;
import java.time.Instant;

import org.opensearch.test.OpenSearchTestCase;

public class AnomalyTests extends OpenSearchTestCase {

    public void testConstructorRejectsEndBeforeStart() {
        Instant start = Instant.parse("2025-03-01T09:00:00Z");
        Instant end = start.minusSeconds(1);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new Anomaly("id-1", "detector-1", start, end)
        );

        assertEquals("dataEndTime must be after dataStartTime", exception.getMessage());
    }

    public void testConstructorRejectsEndEqualStart() {
        Instant start = Instant.parse("2025-03-01T09:00:00Z");

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new Anomaly("id-1", "detector-1", start, start)
        );

        assertEquals("dataEndTime must be after dataStartTime", exception.getMessage());
    }

    public void testGetConfigIdAndToString() {
        Instant start = Instant.parse("2025-03-01T09:00:00Z");
        Instant end = start.plus(Duration.ofMinutes(5));
        Anomaly anomaly = new Anomaly("id-1", "detector-1", start, end);

        assertEquals("detector-1", anomaly.getConfigId());
        assertEquals(
            "Anomaly{id='id-1', detectorName='detector-1', dataStartTime=2025-03-01T09:00:00Z, dataEndTime=2025-03-01T09:05:00Z}",
            anomaly.toString()
        );
    }
}
