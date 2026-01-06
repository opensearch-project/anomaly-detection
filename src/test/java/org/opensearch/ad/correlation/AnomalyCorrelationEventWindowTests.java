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

import java.time.Instant;

import org.opensearch.test.OpenSearchTestCase;

public class AnomalyCorrelationEventWindowTests extends OpenSearchTestCase {

    public void testConstructorRejectsNullStart() {
        Instant end = Instant.parse("2025-03-01T09:05:00Z");

        NullPointerException exception = expectThrows(NullPointerException.class, () -> new AnomalyCorrelation.EventWindow(null, end));

        assertEquals("start", exception.getMessage());
    }

    public void testConstructorRejectsNullEnd() {
        Instant start = Instant.parse("2025-03-01T09:00:00Z");

        NullPointerException exception = expectThrows(NullPointerException.class, () -> new AnomalyCorrelation.EventWindow(start, null));

        assertEquals("end", exception.getMessage());
    }

    public void testConstructorRejectsEndBeforeStart() {
        Instant start = Instant.parse("2025-03-01T09:00:00Z");
        Instant end = start.minusSeconds(1);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new AnomalyCorrelation.EventWindow(start, end)
        );

        assertEquals("end must be on or after start", exception.getMessage());
    }

    public void testAllowsEqualStartAndEnd() {
        Instant start = Instant.parse("2025-03-01T09:00:00Z");
        AnomalyCorrelation.EventWindow window = new AnomalyCorrelation.EventWindow(start, start);

        assertEquals(start, window.getStart());
        assertEquals(start, window.getEnd());
    }

    public void testEqualsHashCodeAndToString() {
        Instant start = Instant.parse("2025-03-01T09:00:00Z");
        Instant end = Instant.parse("2025-03-01T09:05:00Z");

        AnomalyCorrelation.EventWindow window = new AnomalyCorrelation.EventWindow(start, end);
        AnomalyCorrelation.EventWindow same = new AnomalyCorrelation.EventWindow(start, end);
        AnomalyCorrelation.EventWindow different = new AnomalyCorrelation.EventWindow(start, end.plusSeconds(1));

        assertEquals(window, window);
        assertNotEquals(window, null);
        assertNotEquals(window, "not-a-window");
        assertEquals(window, same);
        assertEquals(window.hashCode(), same.hashCode());
        assertNotEquals(window, different);

        assertEquals("EventWindow{start=2025-03-01T09:00:00Z, end=2025-03-01T09:05:00Z}", window.toString());
    }
}
