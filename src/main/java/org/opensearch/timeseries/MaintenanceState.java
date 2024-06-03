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

package org.opensearch.timeseries;

import java.time.Duration;
import java.util.Map;

/**
 * Represent a state that needs to maintain its metadata regularly
 *
 *
 */
public interface MaintenanceState {
    default <K, V extends ExpiringState> void maintenance(Map<K, V> stateToClean, Duration stateTtl) {
        stateToClean.entrySet().stream().forEach(entry -> {
            K configId = entry.getKey();

            V state = entry.getValue();
            if (state.expired(stateTtl)) {
                stateToClean.remove(configId);
            }

        });
    }

    void maintenance();
}
