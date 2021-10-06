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

package org.opensearch.ad.caching;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import org.opensearch.ad.ExpiringState;
import org.opensearch.ad.MaintenanceState;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * A bloom filter with regular reset.
 *
 * Reference: https://arxiv.org/abs/1512.00727
 *
 */
public class DoorKeeper implements MaintenanceState, ExpiringState {
    // stores entity's model id
    private BloomFilter<String> bloomFilter;
    // the number of expected insertions to the constructed BloomFilter<T>; must be positive
    private final long expectedInsertions;
    // the desired false positive probability (must be positive and less than 1.0)
    private final double fpp;
    private Instant lastMaintenanceTime;
    private final Duration resetInterval;
    private final Clock clock;
    private Instant lastAccessTime;

    public DoorKeeper(long expectedInsertions, double fpp, Duration resetInterval, Clock clock) {
        this.expectedInsertions = expectedInsertions;
        this.fpp = fpp;
        this.resetInterval = resetInterval;
        this.clock = clock;
        this.lastAccessTime = clock.instant();
        maintenance();
    }

    public boolean mightContain(String modelId) {
        this.lastAccessTime = clock.instant();
        return bloomFilter.mightContain(modelId);
    }

    public boolean put(String modelId) {
        this.lastAccessTime = clock.instant();
        return bloomFilter.put(modelId);
    }

    /**
     * We reset the bloom filter when bloom filter is null or it is state ttl is reached
     */
    @Override
    public void maintenance() {
        if (bloomFilter == null || lastMaintenanceTime.plus(resetInterval).isBefore(clock.instant())) {
            bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charsets.US_ASCII), expectedInsertions, fpp);
            lastMaintenanceTime = clock.instant();
        }
    }

    @Override
    public boolean expired(Duration stateTtl) {
        return expired(lastAccessTime, stateTtl, clock.instant());
    }
}
