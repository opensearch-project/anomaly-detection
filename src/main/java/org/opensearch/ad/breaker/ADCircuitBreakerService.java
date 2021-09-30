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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.monitor.jvm.JvmService;

/**
 * Class {@code ADCircuitBreakerService} provide storing, retrieving circuit breakers functions.
 *
 * This service registers internal system breakers and provide API for users to register their own breakers.
 */
public class ADCircuitBreakerService {

    private final ConcurrentMap<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();
    private final JvmService jvmService;

    private static final Logger logger = LogManager.getLogger(ADCircuitBreakerService.class);

    /**
     * Constructor.
     *
     * @param jvmService jvm info
     */
    public ADCircuitBreakerService(JvmService jvmService) {
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
    public ADCircuitBreakerService init() {
        // Register memory circuit breaker
        registerBreaker(BreakerName.MEM.getName(), new MemoryCircuitBreaker(this.jvmService));
        logger.info("Registered memory breaker.");

        return this;
    }

    public Boolean isOpen() {
        if (!EnabledSetting.isADBreakerEnabled()) {
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
