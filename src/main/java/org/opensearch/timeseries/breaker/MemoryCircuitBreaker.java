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

package org.opensearch.timeseries.breaker;

import org.opensearch.monitor.jvm.JvmService;

/**
 * A circuit breaker for memory usage.
 */
public class MemoryCircuitBreaker extends ThresholdCircuitBreaker<Short> {

    public static final short DEFAULT_JVM_HEAP_USAGE_THRESHOLD = 95;
    private final JvmService jvmService;

    public MemoryCircuitBreaker(JvmService jvmService) {
        super(DEFAULT_JVM_HEAP_USAGE_THRESHOLD);
        this.jvmService = jvmService;
    }

    public MemoryCircuitBreaker(short threshold, JvmService jvmService) {
        super(threshold);
        this.jvmService = jvmService;
    }

    @Override
    public boolean isOpen() {
        return jvmService.stats().getMem().getHeapUsedPercent() > this.getThreshold();
    }
}
