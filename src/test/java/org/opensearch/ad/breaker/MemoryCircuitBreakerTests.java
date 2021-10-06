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

package org.opensearch.ad.breaker;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.jvm.JvmStats;

public class MemoryCircuitBreakerTests {

    @Mock
    JvmService jvmService;

    @Mock
    JvmStats jvmStats;

    @Mock
    JvmStats.Mem mem;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(jvmService.stats()).thenReturn(jvmStats);
        when(jvmStats.getMem()).thenReturn(mem);
        when(mem.getHeapUsedPercent()).thenReturn((short) 50);
    }

    @Test
    public void testIsOpen() {
        CircuitBreaker breaker = new MemoryCircuitBreaker(jvmService);

        assertThat(breaker.isOpen(), equalTo(false));
    }

    @Test
    public void testIsOpen1() {
        CircuitBreaker breaker = new MemoryCircuitBreaker((short) 90, jvmService);

        assertThat(breaker.isOpen(), equalTo(false));
    }

    @Test
    public void testIsOpen2() {
        CircuitBreaker breaker = new MemoryCircuitBreaker(jvmService);

        when(mem.getHeapUsedPercent()).thenReturn((short) 95);
        assertThat(breaker.isOpen(), equalTo(true));
    }

    @Test
    public void testIsOpen3() {
        CircuitBreaker breaker = new MemoryCircuitBreaker((short) 90, jvmService);

        when(mem.getHeapUsedPercent()).thenReturn((short) 95);
        assertThat(breaker.isOpen(), equalTo(true));
    }
}
