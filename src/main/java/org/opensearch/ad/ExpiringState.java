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

package org.opensearch.ad;

import java.time.Duration;
import java.time.Instant;

/**
 * Represent a state that can be expired with a duration if not accessed
 *
 */
public interface ExpiringState {
    default boolean expired(Instant lastAccessTime, Duration stateTtl, Instant now) {
        return lastAccessTime.plus(stateTtl).isBefore(now);
    }

    boolean expired(Duration stateTtl);
}
