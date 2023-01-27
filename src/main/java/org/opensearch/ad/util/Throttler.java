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

package org.opensearch.ad.util;

import java.time.Clock;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.opensearch.action.ActionRequest;

/**
 * Utility functions for throttling query.
 */
public class Throttler {
    // negativeCache is used to reject search query if given detector already has one query running
    // key is detectorId, value is an entry. Key is ActionRequest and value is the timestamp
    private final ConcurrentHashMap<String, Map.Entry<ActionRequest, Instant>> negativeCache;
    private final Clock clock;

    public Throttler(Clock clock) {
        this.negativeCache = new ConcurrentHashMap<>();
        this.clock = clock;
    }

    /**
     * This will be used when dependency injection directly/indirectly injects a Throttler object. Without this object,
     * node start might fail due to not being able to find a Clock object. We removed Clock object association in
     * https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/305
     */
    public Throttler() {
        this(Clock.systemUTC());
    }

    /**
     * Get negative cache value(ActionRequest, Instant) for given detector
     * @param detectorId AnomalyDetector ID
     * @return negative cache value(ActionRequest, Instant)
     */
    public Optional<Map.Entry<ActionRequest, Instant>> getFilteredQuery(String detectorId) {
        return Optional.ofNullable(negativeCache.get(detectorId));
    }

    /**
     * Insert the negative cache entry for given detector
     * If key already exists, return false. Otherwise true.
     * @param detectorId AnomalyDetector ID
     * @param request ActionRequest
     * @return true if key doesn't exist otherwise false.
     */
    public synchronized boolean insertFilteredQuery(String detectorId, ActionRequest request) {
        return negativeCache.putIfAbsent(detectorId, new AbstractMap.SimpleEntry<>(request, clock.instant())) == null;
    }

    /**
     * Clear the negative cache for given detector.
     * @param detectorId AnomalyDetector ID
     */
    public void clearFilteredQuery(String detectorId) {
        negativeCache.remove(detectorId);
    }
}
