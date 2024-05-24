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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.timeseries.settings.TimeSeriesEnabledSetting;

/**
 * Class {@code CircuitBreakerService} provide storing, retrieving circuit breakers functions.
 *
 * This service registers internal system breakers and provide API for users to register their own breakers.
 */
public class CircuitBreakerService {

    private final ConcurrentMap<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();
    private final JvmService jvmService;

    private static final Logger logger = LogManager.getLogger(CircuitBreakerService.class);

    /**
     * Constructor.
     *
     * @param jvmService jvm info
     */
    public CircuitBreakerService(JvmService jvmService) {
        this.jvmService = jvmService;
    }

    public void registerBreaker(String name, CircuitBreaker breaker) {
        breakers.putIfAbsent(name, breaker);
    }

    public void unregisterBreaker(String name) {
        if (name == null) {
            return;
        }

        breakers.remove(name);
    }

    public void clearBreakers() {
        breakers.clear();
    }

    public CircuitBreaker getBreaker(String name) {
        return breakers.get(name);
    }

    /**
     * Initialize circuit breaker service.
     *
     * Register memory breaker by default.
     *
     * @return ADCircuitBreakerService
     */
    public CircuitBreakerService init() {
        // Register memory circuit breaker
        registerBreaker(BreakerName.MEM.getName(), new MemoryCircuitBreaker(this.jvmService));
        logger.info("Registered memory breaker.");

        return this;
    }

    public Boolean isOpen() {
        if (!TimeSeriesEnabledSetting.isBreakerEnabled()) {
            return false;
        }

        for (CircuitBreaker breaker : breakers.values()) {
            if (breaker.isOpen()) {
                return true;
            }
        }

        return false;
    }
}
