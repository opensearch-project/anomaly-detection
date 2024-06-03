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

package org.opensearch.ad.stats.suppliers;

import org.junit.Test;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.stats.suppliers.CounterSupplier;

public class CounterSupplierTests extends OpenSearchTestCase {
    @Test
    public void testGetAndIncrement() {
        CounterSupplier counterSupplier = new CounterSupplier();
        assertEquals("get returns incorrect value", (Long) 0L, counterSupplier.get());
        counterSupplier.increment();
        assertEquals("get returns incorrect value", (Long) 1L, counterSupplier.get());
    }
}
