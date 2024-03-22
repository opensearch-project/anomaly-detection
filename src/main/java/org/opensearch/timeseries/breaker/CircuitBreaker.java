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
 * An interface for circuit breaker.
 *
 * We use circuit breaker to protect a certain system resource like memory, cpu etc.
 */
public interface CircuitBreaker {

    boolean isOpen();
}
