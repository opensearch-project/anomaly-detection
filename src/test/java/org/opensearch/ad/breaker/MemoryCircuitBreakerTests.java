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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.timeseries.breaker.CircuitBreaker;
import org.opensearch.timeseries.breaker.MemoryCircuitBreaker;

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
    public void testIsOpen_whenUsageIsBelowDefaultValue_shouldReturnFalse() {
        CircuitBreaker breaker = new MemoryCircuitBreaker(jvmService);

        assertThat(breaker.isOpen(), equalTo(false));
    }

    @Test
    public void testIsOpen_whenUsageIsAboveDefaultValue_shouldReturnTrue() {
        CircuitBreaker breaker = new MemoryCircuitBreaker(jvmService);

        doReturn((short) 96).when(mem).getHeapUsedPercent();
        assertThat(breaker.isOpen(), equalTo(true));
    }

    @Test
    public void testIsOpen_whenUsageIsAboveSettingValue_shouldReturnTrue() {
        CircuitBreaker breaker = new MemoryCircuitBreaker(90, jvmService);

        doReturn((short) 96).when(mem).getHeapUsedPercent();
        assertThat(breaker.isOpen(), equalTo(true));
    }

    @Test
    public void testIsOpen_whenUsageIsBelowSettingValue_butAboveDefaultValue_shouldReturnFalse() {
        CircuitBreaker breaker = new MemoryCircuitBreaker(97, jvmService);

        doReturn((short) 96).when(mem).getHeapUsedPercent();
        assertThat(breaker.isOpen(), equalTo(false));
    }
}
