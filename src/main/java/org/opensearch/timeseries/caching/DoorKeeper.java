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

package org.opensearch.timeseries.caching;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.timeseries.ExpiringState;
import org.opensearch.timeseries.MaintenanceState;

/**
 * A hashmap thats track the exact frequency of each element and reset regularly.
 *
 * The name of door keeper derives from https://arxiv.org/abs/1512.00727
 *
 */
public class DoorKeeper implements MaintenanceState, ExpiringState {
    private final Logger LOG = LogManager.getLogger(DoorKeeper.class);
    // stores entity's model id
    private final long expectedInsertions;
    private Map<String, Integer> frequencyMap;
    private Instant lastMaintenanceTime;
    private final Duration resetInterval;
    private final Clock clock;
    private Instant lastAccessTime;
    private final int countThreshold;

    public DoorKeeper(long expectedInsertions, Duration resetInterval, Clock clock, int countThreshold) {
        this.expectedInsertions = expectedInsertions;
        this.resetInterval = resetInterval;
        this.clock = clock;
        this.countThreshold = countThreshold;
        this.lastAccessTime = clock.instant();
        maintenance();
    }

    public void put(String modelId) {
        this.lastAccessTime = clock.instant();
        this.frequencyMap.put(modelId, this.frequencyMap.getOrDefault(modelId, 0) + 1);
        if (frequencyMap.size() > expectedInsertions) {
            reset();
        }
    }

    /**
     * We reset the bloom filter when bloom filter is null or it is state ttl is reached
     */
    @Override
    public void maintenance() {
        if (frequencyMap == null || lastMaintenanceTime.plus(resetInterval).isBefore(clock.instant())) {
            LOG.debug("maintaining for doorkeeper");
            reset();
        }
    }

    private void reset() {
        frequencyMap = new HashMap<>();
        lastMaintenanceTime = clock.instant();
    }

    public boolean appearsMoreThanOrEqualToThreshold(String item) {
        this.lastAccessTime = clock.instant();
        return this.frequencyMap.getOrDefault(item, 0) >= countThreshold;
    }

    @Override
    public boolean expired(Duration stateTtl) {
        // ignore stateTtl since we have customized resetInterval
        return expired(lastAccessTime, resetInterval, clock.instant());
    }
}
