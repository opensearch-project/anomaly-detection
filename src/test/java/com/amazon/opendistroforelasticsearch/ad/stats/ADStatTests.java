/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.stats;

import java.util.function.Supplier;

import org.junit.Test;
import org.opensearch.test.OpenSearchTestCase;

import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.CounterSupplier;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.SettableSupplier;

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
