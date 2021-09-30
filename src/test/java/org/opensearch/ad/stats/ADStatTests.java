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
import org.opensearch.ad.stats.suppliers.CounterSupplier;
import org.opensearch.ad.stats.suppliers.SettableSupplier;
import org.opensearch.test.OpenSearchTestCase;

public class ADStatTests extends OpenSearchTestCase {

    @Test
    public void testIsClusterLevel() {
        ADStat<String> stat1 = new ADStat<>(true, new TestSupplier());
        assertTrue("isCluster returns the wrong value", stat1.isClusterLevel());
        ADStat<String> stat2 = new ADStat<>(false, new TestSupplier());
        assertTrue("isCluster returns the wrong value", !stat2.isClusterLevel());
    }

    @Test
    public void testGetValue() {
        ADStat<Long> stat1 = new ADStat<>(false, new CounterSupplier());
        assertEquals("GetValue returns the incorrect value", 0L, (long) (stat1.getValue()));

        ADStat<String> stat2 = new ADStat<>(false, new TestSupplier());
        assertEquals("GetValue returns the incorrect value", "test", stat2.getValue());
    }

    @Test
    public void testSetValue() {
        ADStat<Long> stat = new ADStat<>(false, new SettableSupplier());
        assertEquals("GetValue returns the incorrect value", 0L, (long) (stat.getValue()));
        stat.setValue(10L);
        assertEquals("GetValue returns the incorrect value", 10L, (long) stat.getValue());
    }

    @Test
    public void testIncrement() {
        ADStat<Long> incrementStat = new ADStat<>(false, new CounterSupplier());

        for (Long i = 0L; i < 100; i++) {
            assertEquals("increment does not work", i, incrementStat.getValue());
            incrementStat.increment();
        }

        // Ensure that no problems occur for a stat that cannot be incremented
        ADStat<String> nonIncStat = new ADStat<>(false, new TestSupplier());
        nonIncStat.increment();
    }

    private class TestSupplier implements Supplier<String> {
        TestSupplier() {}

        public String get() {
            return "test";
        }
    }
}
