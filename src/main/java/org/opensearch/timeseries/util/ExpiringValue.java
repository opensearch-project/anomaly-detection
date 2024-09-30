/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import java.time.Clock;

public class ExpiringValue<V> {
    private V value;
    private long lastAccessTime;
    private long expirationTimeInMillis;
    private Clock clock;

    public ExpiringValue(V value, long expirationTimeInMillis, Clock clock) {
        this.value = value;
        this.expirationTimeInMillis = expirationTimeInMillis;
        this.clock = clock;
        updateLastAccessTime();
    }

    public V getValue() {
        updateLastAccessTime();
        return value;
    }

    public boolean isExpired() {
        return isExpired(clock.millis());
    }

    public boolean isExpired(long currentTimeMillis) {
        return (currentTimeMillis - lastAccessTime) >= expirationTimeInMillis;
    }

    public void updateLastAccessTime() {
        lastAccessTime = clock.millis();
    }
}
