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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.timeseries.breaker.BreakerName;
import org.opensearch.timeseries.breaker.CircuitBreaker;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.breaker.MemoryCircuitBreaker;

public class ADCircuitBreakerServiceTests {

    @InjectMocks
    private CircuitBreakerService adCircuitBreakerService;

    @Mock
    JvmService jvmService;

    @Mock
    JvmStats jvmStats;

    @Mock
    JvmStats.Mem mem;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testRegisterBreaker() {
        adCircuitBreakerService.registerBreaker(BreakerName.MEM.getName(), new MemoryCircuitBreaker(jvmService));
        CircuitBreaker breaker = adCircuitBreakerService.getBreaker(BreakerName.MEM.getName());

        assertThat(breaker, is(notNullValue()));
    }

    @Test
    public void testRegisterBreakerNull() {
        CircuitBreaker breaker = adCircuitBreakerService.getBreaker(BreakerName.MEM.getName());

        assertThat(breaker, is(nullValue()));
    }

    @Test
    public void testUnregisterBreaker() {
        adCircuitBreakerService.registerBreaker(BreakerName.MEM.getName(), new MemoryCircuitBreaker(jvmService));
        CircuitBreaker breaker = adCircuitBreakerService.getBreaker(BreakerName.MEM.getName());
        assertThat(breaker, is(notNullValue()));
        adCircuitBreakerService.unregisterBreaker(BreakerName.MEM.getName());
        breaker = adCircuitBreakerService.getBreaker(BreakerName.MEM.getName());
        assertThat(breaker, is(nullValue()));
    }

    @Test
    public void testUnregisterBreakerNull() {
        adCircuitBreakerService.registerBreaker(BreakerName.MEM.getName(), new MemoryCircuitBreaker(jvmService));
        adCircuitBreakerService.unregisterBreaker(null);
        CircuitBreaker breaker = adCircuitBreakerService.getBreaker(BreakerName.MEM.getName());
        assertThat(breaker, is(notNullValue()));
    }

    @Test
    public void testClearBreakers() {
        adCircuitBreakerService.registerBreaker(BreakerName.CPU.getName(), new MemoryCircuitBreaker(jvmService));
        CircuitBreaker breaker = adCircuitBreakerService.getBreaker(BreakerName.CPU.getName());
        assertThat(breaker, is(notNullValue()));
        adCircuitBreakerService.clearBreakers();
        breaker = adCircuitBreakerService.getBreaker(BreakerName.CPU.getName());
        assertThat(breaker, is(nullValue()));
    }

    @Test
    public void testInit() {
        assertThat(adCircuitBreakerService.init(), is(notNullValue()));
    }

    @Test
    public void testIsOpen() {
        when(jvmService.stats()).thenReturn(jvmStats);
        when(jvmStats.getMem()).thenReturn(mem);
        when(mem.getHeapUsedPercent()).thenReturn((short) 50);

        adCircuitBreakerService.registerBreaker(BreakerName.MEM.getName(), new MemoryCircuitBreaker(jvmService));
        assertThat(adCircuitBreakerService.isOpen(), equalTo(false));
    }

    @Test
    public void testIsOpen1() {
        when(jvmService.stats()).thenReturn(jvmStats);
        when(jvmStats.getMem()).thenReturn(mem);
        when(mem.getHeapUsedPercent()).thenReturn((short) 96);

        adCircuitBreakerService.registerBreaker(BreakerName.MEM.getName(), new MemoryCircuitBreaker(jvmService));
        assertThat(adCircuitBreakerService.isOpen(), equalTo(true));
    }
}
