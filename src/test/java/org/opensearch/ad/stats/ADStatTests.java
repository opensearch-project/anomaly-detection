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

package org.opensearch.ad.stats;

import java.util.function.Supplier;

import org.junit.Test;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.stats.TimeSeriesStat;
import org.opensearch.timeseries.stats.suppliers.CounterSupplier;
import org.opensearch.timeseries.stats.suppliers.SettableSupplier;

public class ADStatTests extends OpenSearchTestCase {

    @Test
    public void testIsClusterLevel() {
        TimeSeriesStat<String> stat1 = new TimeSeriesStat<>(true, new TestSupplier());
        assertTrue("isCluster returns the wrong value", stat1.isClusterLevel());
        TimeSeriesStat<String> stat2 = new TimeSeriesStat<>(false, new TestSupplier());
        assertTrue("isCluster returns the wrong value", !stat2.isClusterLevel());
    }

    @Test
    public void testGetValue() {
        TimeSeriesStat<Long> stat1 = new TimeSeriesStat<>(false, new CounterSupplier());
        assertEquals("GetValue returns the incorrect value", 0L, (long) (stat1.getValue()));

        TimeSeriesStat<String> stat2 = new TimeSeriesStat<>(false, new TestSupplier());
        assertEquals("GetValue returns the incorrect value", "test", stat2.getValue());
    }

    @Test
    public void testSetValue() {
        TimeSeriesStat<Long> stat = new TimeSeriesStat<>(false, new SettableSupplier());
        assertEquals("GetValue returns the incorrect value", 0L, (long) (stat.getValue()));
        stat.setValue(10L);
        assertEquals("GetValue returns the incorrect value", 10L, (long) stat.getValue());
    }

    @Test
    public void testIncrement() {
        TimeSeriesStat<Long> incrementStat = new TimeSeriesStat<>(false, new CounterSupplier());

        for (Long i = 0L; i < 100; i++) {
            assertEquals("increment does not work", i, incrementStat.getValue());
            incrementStat.increment();
        }

        // Ensure that no problems occur for a stat that cannot be incremented
        TimeSeriesStat<String> nonIncStat = new TimeSeriesStat<>(false, new TestSupplier());
        nonIncStat.increment();
    }

    private class TestSupplier implements Supplier<String> {
        TestSupplier() {}

        public String get() {
            return "test";
        }
    }
}
