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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.caching;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import com.amazon.opendistroforelasticsearch.ad.ExpiringState;
import com.amazon.opendistroforelasticsearch.ad.MaintenanceState;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * A bloom filter placed in front of inactive entity cache to
 * filter out unpopular items that are not likely to appear more
 * than once.
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
