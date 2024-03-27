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

/**
 * An abstract class for all breakers with threshold.
 * @param <T> data type of threshold
 */
public abstract class ThresholdCircuitBreaker<T> implements CircuitBreaker {

    private T threshold;

    public ThresholdCircuitBreaker(T threshold) {
        this.threshold = threshold;
    }

    public T getThreshold() {
        return threshold;
    }

    @Override
    public abstract boolean isOpen();
}
