/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.ml;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.constant.CommonName;

/** Unit tests for {@link Sample}. */
public class SampleTests extends OpenSearchTestCase {

    /* ------------------------------------------------------------------
       toString() contains all fields
       ------------------------------------------------------------------ */
    public void testToStringContainsFields() {
        double[] values = new double[] { 1.1, 2.2 };
        Instant start = Instant.now().minusSeconds(60);
        Instant end = Instant.now();

        Sample s = new Sample(values, start, end);
        String str = s.toString();

        assertTrue("toString lacks data array", str.contains("[1.1, 2.2]"));
        assertTrue("toString lacks start time", str.contains(start.toString()));
        assertTrue("toString lacks end time", str.contains(end.toString()));
    }

    /* ------------------------------------------------------------------
       isInvalid()  –  positive & negative cases
       ------------------------------------------------------------------ */
    public void testIsInvalid() {
        // default ctor ⇒ invalid
        Sample invalid = new Sample();
        assertTrue("Expected invalid sample", invalid.isInvalid());

        // normal ctor ⇒ valid
        Sample valid = new Sample(new double[] { 0.0 }, Instant.EPOCH, Instant.EPOCH.plusSeconds(1));
        assertFalse("Expected valid sample", valid.isInvalid());
    }

    /* ------------------------------------------------------------------
       extractSample()  →  null when all keys missing
       ------------------------------------------------------------------ */
    public void testExtractSampleReturnsNullWhenKeysMissing() {
        Map<String, Object> empty = new HashMap<>();
        assertNull("Expected null when map has no required keys", Sample.extractSample(empty));
    }

    /* ------------------------------------------------------------------
       extractSample()  –  happy path sanity-check
       ------------------------------------------------------------------ */
    public void testExtractSampleParsesCorrectly() {
        Instant start = Instant.now().minusSeconds(30);
        Instant end = Instant.now();

        Map<String, Object> map = new HashMap<>();
        map.put(CommonName.DATA_START_TIME_FIELD, start.toEpochMilli());
        map.put(CommonName.DATA_END_TIME_FIELD, end.toEpochMilli());
        map.put(CommonName.VALUE_LIST_FIELD, List.of(3.3, 4.4));

        Sample parsed = Sample.extractSample(map);
        assertNotNull(parsed);
        assertArrayEquals(new double[] { 3.3, 4.4 }, parsed.getValueList(), 0.0001);
        assertEquals(
            start.truncatedTo(java.time.temporal.ChronoUnit.MILLIS),
            parsed.getDataStartTime().truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
        );
        assertEquals(
            end.truncatedTo(java.time.temporal.ChronoUnit.MILLIS),
            parsed.getDataEndTime().truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
        );
    }
}
